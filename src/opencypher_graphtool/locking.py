# Copyright 2026 S. Alex Thompson
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Concurrency: one fair readers-writer lock per graph."""
from __future__ import annotations

import asyncio
import collections
import contextlib
import threading
from collections import Counter
from graph_tool import Graph
from typing import Callable
from .errors import LockTimeout, LockUpgradeError


class _Waiter:
    __slots__ = ("write", "grant", "granted")

    def __init__(self, write: bool, grant: Callable[[], None]):
        self.write, self.grant, self.granted = write, grant, False


class _Hold:
    """Proof that a lock is held. `lock is None` marks an inert hold (nested / disabled)."""
    __slots__ = ("lock", "write", "active")

    def __init__(self, lock: "GraphLock | None", write: bool):
        self.lock, self.write, self.active = lock, write, lock is not None


class GraphLock:
    """Fair (strict FIFO) readers-writer lock, one per graph, shared by everything that drives the same Graph.

    * Any number of readers run together; a writer runs alone. Waiters are served in arrival
      order, so a stream of readers cannot starve a queued writer and writers run one by one.
    * Threads call `with lock.read():` / `with lock.write():` (the engines do this for you);
      asyncio code awaits `lock.acquire_async()` / `async with lock.write_async():`, which
      queues WITHOUT blocking the event loop. Sync and async waiters share one queue.
    * Re-entrant for the thread that holds it: a nested read or write inside a write is fine,
      a nested read inside a read is fine, and a write inside a read raises LockUpgradeError
      (waiting for it would deadlock) instead of hanging.
    * `enabled = False` turns every acquire into a no-op.
    """
    def __init__(self, enabled: bool = True):
        self.enabled = enabled
        self._mu = threading.Lock()
        self._readers = 0
        self._writer = False
        self._queue: collections.deque = collections.deque()
        self._tl = threading.local()
        self.stats: Counter = Counter()      # reads, writes, waits, max_queue

    # -- introspection ------------------------------------------------------ #
    def queue_length(self) -> int:
        return len(self._queue)

    def locked(self) -> bool:
        return self._writer or self._readers > 0

    def _current(self) -> "_Hold | None":
        h = getattr(self._tl, "hold", None)
        return h if (h is not None and h.active) else None

    def mode(self) -> "str | None":
        """'w' / 'r' if THIS thread holds the lock, else None."""
        h = self._current()
        return None if h is None else ("w" if h.write else "r")

    # -- core ------------------------------------------------------------------ #
    def _pump(self) -> None:                  # caller holds _mu
        while self._queue:
            w = self._queue[0]
            if w.write:
                if self._writer or self._readers:
                    return
                self._writer = True
            else:
                if self._writer:
                    return
                self._readers += 1
            self._queue.popleft()
            w.granted = True
            w.grant()

    def _enqueue(self, w: _Waiter) -> bool:
        with self._mu:
            self._queue.append(w)
            self.stats["max_queue"] = max(self.stats["max_queue"], len(self._queue))
            self._pump()
            return not w.granted              # True = had to wait

    def _inert_or_error(self, write: bool) -> "_Hold | None":
        if not self.enabled:
            return _Hold(None, write)
        cur = self._current()
        if cur is None:
            return None
        if cur.write or not write:
            return _Hold(None, write)         # re-entrant: nothing to acquire
        raise LockUpgradeError(
            "a write was requested while this thread holds a READ lock; waiting would deadlock. "
            "Take the write lock for the whole block instead (`with lock.write():`).")

    def _granted(self, write: bool) -> _Hold:
        hold = _Hold(self, write)
        self.stats["writes" if write else "reads"] += 1
        return hold

    def acquire(self, write: bool = False, timeout: float | None = None) -> _Hold:
        inert = self._inert_or_error(write)
        if inert is not None:
            return inert
        ev = threading.Event()
        w = _Waiter(write, ev.set)
        if self._enqueue(w):
            self.stats["waits"] += 1
            if not ev.wait(timeout):
                with self._mu:
                    if not w.granted:
                        self._queue.remove(w)
                        self._pump()
                        raise LockTimeout(f"{'write' if write else 'read'} lock not granted within {timeout}s")
        hold = self._granted(write)
        self._tl.hold = hold
        return hold

    def release(self, hold: _Hold) -> None:
        if hold.lock is None or not hold.active:
            return
        hold.active = False
        with self._mu:
            if hold.write:
                self._writer = False
            else:
                self._readers -= 1
            self._pump()

    @contextlib.contextmanager
    def read(self, timeout: float | None = None):
        h = self.acquire(False, timeout)
        try:
            yield h
        finally:
            self.release(h)

    @contextlib.contextmanager
    def write(self, timeout: float | None = None):
        h = self.acquire(True, timeout)
        try:
            yield h
        finally:
            self.release(h)

    # -- asyncio ----------------------------------------------------------------- #
    async def acquire_async(self, write: bool = False) -> _Hold:
        """Queue for the lock without blocking the event loop. The returned hold is NOT tied to
        a thread: run work with `run_in_thread(hold, fn)` so nested engine calls are re-entrant."""
        inert = self._inert_or_error(write)
        if inert is not None:
            return inert
        loop = asyncio.get_running_loop()
        fut = loop.create_future()

        def grant():
            loop.call_soon_threadsafe(lambda: fut.done() or fut.set_result(None))
        w = _Waiter(write, grant)
        if self._enqueue(w):
            self.stats["waits"] += 1
        try:
            await fut
        except asyncio.CancelledError:
            with self._mu:
                if not w.granted:                 # still queued: just leave the queue
                    self._queue.remove(w)
                    self._pump()
                    raise
            self.release(_Hold(self, write))      # granted just as we were cancelled: give it back
            raise
        return self._granted(write)

    @contextlib.asynccontextmanager
    async def read_async(self):
        h = await self.acquire_async(False)
        try:
            yield h
        finally:
            self.release(h)

    @contextlib.asynccontextmanager
    async def write_async(self):
        h = await self.acquire_async(True)
        try:
            yield h
        finally:
            self.release(h)

    @contextlib.contextmanager
    def adopt(self, hold: _Hold):
        """Make `hold` this thread's current hold (used when a coroutine's lock is exercised
        in a worker thread)."""
        prev = getattr(self._tl, "hold", None)
        self._tl.hold = hold
        try:
            yield
        finally:
            self._tl.hold = prev

    async def run_in_thread(self, hold: _Hold, fn: Callable, *args, **kw):
        loop = asyncio.get_running_loop()

        def call():
            with self.adopt(hold):
                return fn(*args, **kw)
        return await loop.run_in_executor(None, call)


_LOCK_ATTR = "_gt_graph_lock"


def lock_of(graph: Graph) -> GraphLock:
    """The graph's lock (created on first use, shared by every engine on that graph)."""
    lk = graph.__dict__.get(_LOCK_ATTR)
    if lk is None:
        lk = graph.__dict__.setdefault(_LOCK_ATTR, GraphLock())
    return lk


def _check_write(g: Graph) -> None:
    """Safety net: a mutation reached while this thread holds only a READ lock means a write was
    not detected up front (e.g. hidden inside a Python callback). Fail loudly, never race."""
    lk = g.__dict__.get(_LOCK_ATTR)
    if lk is not None and lk.enabled and lk.mode() == "r":
        raise LockUpgradeError(
            "a mutation happened inside a query that holds only a read lock (its writes were not "
            "visible to the engine, e.g. a Python callback). Wrap the whole block in "
            "`with lock.write():` or run it as an explicit write query.")
