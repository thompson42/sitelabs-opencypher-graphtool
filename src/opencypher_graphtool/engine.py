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

"""The public engine: OpenCypher, opencypher(), VerifyReport."""
from __future__ import annotations

import asyncio
import collections
import logging
from collections import Counter
from graph_tool import Graph
from typing import Any
from typing import Callable
from typing import Iterable
from .errors import ConsistencyError, OpenCypherError
from .locking import GraphLock, lock_of
from .common import _TXN_ATTR, _Txn
from .ids import bulk_node_id, norm_id, stable_id
from .ast_nodes import Call, Create, Delete, Match, Merge, Proj, Query, Remove, SetC, Unwind, _has_agg
from .parser import parse
from .values import _hk
from .results import Node, Path, Rel, Result
from .models import Models
from .schema import Schema, _SCHEMAS, _STORES
from .elements import _create_edge, _create_vertex, _set_props
from .stores import PropertyStore
from .cassandra_store import _REV_MARKER
from .graph_functions import VECTOR_SCAN_LIMIT
from .clauses import _Run, _delete_elements, _msg, _pattern_vars
from .procedures import VECTOR_SEARCH_OVERSAMPLE

log = logging.getLogger(__name__)

def _norm_or_raw(x: Any) -> str:
    try:
        return norm_id(x)
    except Exception:
        return str(x)


class VerifyReport:
    """Result of OpenCypher.verify(). Truthy when graph-tool and the store agree."""

    def __init__(self):
        self.checked: dict = {}          # (kind, type) -> (ids in graph, ids in store)
        self.missing_rows: dict = {}     # (kind, type) -> [ids in graph with no row]
        self.orphan_rows: dict = {}      # (kind, type) -> [ids with a row but not in graph]
        self.missing_tables: list = []   # (kind, type) with graph elements but no table
        self.unresolved: list = []       # failed rollback corrections still pending
        # id_index="store" only: the reverse id table compared with graph-tool
        self.reverse_missing: list = []  # ids in the graph with no reverse row
        self.reverse_stale: list = []    # reverse row disagrees with the graph (index/endpoints)
        self.reverse_orphans: list = []  # reverse rows whose id is not in the graph
        self.reverse_marker: bool = True # completeness marker present

    @property
    def ok(self) -> bool:
        return not (self.missing_rows or self.orphan_rows or self.missing_tables or self.unresolved
                    or self.reverse_missing or self.reverse_stale or self.reverse_orphans
                    or not self.reverse_marker)

    __bool__ = lambda self: self.ok

    def summary(self) -> str:
        if self.ok:
            return "consistent: " + ", ".join(f"{k[1]}={v[0]}" for k, v in sorted(self.checked.items()))
        parts = []
        for name, d in (("missing rows", self.missing_rows), ("orphan rows", self.orphan_rows)):
            for (kind, tn), ids in sorted(d.items()):
                parts.append(f"{len(ids)} {name} in {kind} table {tn!r}")
        parts += [f"no table for {k} type {t!r}" for k, t in self.missing_tables]
        for name, d in (("missing", self.reverse_missing), ("stale", self.reverse_stale),
                        ("orphan", self.reverse_orphans)):
            if d:
                parts.append(f"{len(d)} {name} reverse-id row(s)")
        if not self.reverse_marker:
            parts.append("reverse id table has no completeness marker")
        if self.unresolved:
            parts.append(f"{len(self.unresolved)} unresolved rollback correction(s)")
        return "; ".join(parts)

    def __repr__(self) -> str:
        return f"<VerifyReport {self.summary()}>"


class OpenCypher:
    """openCypher engine over a graph-tool graph (+ optional external property store).

        cy = opencypher(graph, store, schema)
        cy.run("MATCH (p:Person {name: $n})-[:KNOWS]->(f) RETURN f.name AS friend", n="Ann").data()

    `runner` is the class that runs one query. A subclass can override its matching steps (`seed`,
    `filter_nodes`, `choose_start`, `neighbourhood`) when the graph is a cache of a graph held
    elsewhere (sitelabs-partitioned-graph): see _Run.neighbourhood.
    """
    runner = _Run

    def __init__(self, graph: Graph, store: "PropertyStore | None" = None,
                 schema: Schema | None = None, varlen_cap: int = 64, lock: bool = True,
                 atomic: bool = True, stable_merge_ids: bool = True, id_index: str | None = None,
                 models: "Models | None" = None, migrate: bool = True):
        existing = _SCHEMAS.get(id(graph))
        if schema is None:
            if models is not None:
                schema = (existing[1] if existing else None) or models.schema(graph)
            else:
                schema = (existing[1] if existing else None) or Schema.from_graph(graph) or Schema()
        store_eff = store if store is not None else (_STORES.get(id(graph)) or (None, None))[1]
        if models is not None:
            models.register_into(schema)            # new names get the next FREE bits, none move
            if store_eff is None:
                raise ValueError("models= needs a property store")
            store_eff.attach_models(models)
            if migrate:                             # create / alter the tables from the models
                store_eff.migrate(models, reverse=((id_index or schema._id_mode) == "store"))
        self.models = models
        schema.bind(graph, store=store_eff, id_index=id_index)
        if models is not None:
            schema.attach(graph)                    # the bit map travels with the saved graph
        _SCHEMAS[id(graph)] = (graph, schema)
        if store is not None:
            store.bind(graph)
            _STORES[id(graph)] = (graph, store)
        self.graph, self.schema = graph, schema
        self.store = store if store is not None else (_STORES.get(id(graph)) or (None, None))[1]
        self.varlen_cap = varlen_cap          # depth limit for unbounded * patterns
        lock_of(graph).enabled = bool(lock)
        self.atomic = atomic                  # write queries roll back graph + store on failure
        self.stable_merge_ids = stable_merge_ids   # MERGE-created elements get uuid5 ids
        # on_commit(ops): called once per committed write query, after the store flush and
        # while the write lock is still held. `ops` are the query's graph-tool topology changes
        # as JSON-able lists, keyed by id (never by index):
        #   ["v+", id, [labels], type]   ["e+", id, src_id, dst_id, type]
        #   ["v-", id]   ["e-", id]   ["vl", id, [labels]]
        # Raising rolls the whole query back (graph + store). apply_redo(ops) replays them.
        # Needs atomic=True. Plain graph-tool property maps (no store) are not covered.
        self.on_commit: Callable[[list], None] | None = None
        self.timeout: float | None = None     # seconds per query; QueryTimeout past it
        self.vector_scan_limit: int | None = VECTOR_SCAN_LIMIT   # vector.* calls per query; None: no limit
        self.vector_oversample: int = VECTOR_SEARCH_OVERSAMPLE    # vector.search candidates per result
        self.request_ids: collections.OrderedDict[str, dict] = collections.OrderedDict()   # see run_once
        self.request_id_retain: int = 100_000
        self._request_id: str | None = None   # the run_once() id of the write running now

    def close(self) -> None:
        """Forget this graph's schema/store registration (lets a replaced graph be freed)."""
        for reg in (_SCHEMAS, _STORES):
            entry = reg.get(id(self.graph))
            if entry is not None and entry[0] is self.graph:
                del reg[id(self.graph)]

    def apply_redo(self, ops: Iterable[list]) -> int:
        """Replay on_commit() ops onto graph-tool only (the store already holds the rows).
        Idempotent per op: an element that already exists / is already gone is skipped.
        Store-mode reverse id rows are rewritten to the indices this graph uses.
        Returns the number of ops applied."""
        g, sc, n = self.graph, self.schema, 0
        with self.lock.write():
            for op in ops:
                if op[0] == "rq":                  # an idempotent write's request id (run_once)
                    self.remember_request(op[1], op[2])
                    continue
                tag, id_ = op[0], norm_id(op[1])
                if tag == "v+":
                    if sc.vertex_by_id(g, id_) is not None:
                        continue
                    lm, tm, _ = sc.prepare("vertex", tuple(op[2]), op[3], None)
                    sc.commit(g, "vertex", g.add_vertex(), lm, tm, id_)
                elif tag == "e+":
                    src, dst = sc.vertex_by_id(g, op[2]), sc.vertex_by_id(g, op[3])
                    if sc.edge_by_id(id_) is not None or src is None or dst is None:
                        continue
                    lm, tm, _ = sc.prepare("edge", (), op[4], None)
                    sc.commit(g, "edge", g.add_edge(src, dst), lm, tm, id_)
                elif tag == "v-":
                    v = sc.vertex_by_id(g, id_)
                    if v is None:
                        continue
                    if "_dropped" not in g.vp:
                        g.vp["_dropped"] = g.new_vertex_property("bool")
                    sc.retire(g, "vertex", v)
                    g.vp["_dropped"][v] = True
                elif tag == "e-":
                    e = sc.edge_by_id(id_)
                    if e is None:
                        continue
                    sc.retire(g, "edge", e)
                    g.remove_edge(e)
                elif tag == "vl":
                    v = sc.vertex_by_id(g, id_)
                    if v is None:
                        continue
                    reg, lm = sc.reg("vertex", "label"), 0
                    for l in op[2]:
                        lm |= reg.resolve(l)
                    g.vp[sc.label_map][v] = sc.to_stored(lm)
                else:
                    raise OpenCypherError(f"unknown redo op {tag!r}")
                n += 1
            sc.computed.clear()
            if self.store is not None:
                self.store.flush()               # store mode: the staged reverse id rows
        return n

    @property
    def lock(self) -> GraphLock:
        """The graph's readers-writer lock (one per Graph). Hold it to make several
        queries one atomic block: `with cy.lock.write(): ...` / `async with cy.lock.write_async():`."""
        return lock_of(self.graph)

    @staticmethod
    def is_write(q: "Query | str") -> bool:
        """Does the query change the graph? (CREATE / MERGE / SET / REMOVE / DELETE)"""
        q = parse(q) if isinstance(q, str) else q
        return any(isinstance(c, (Create, Merge, SetC, Remove, Delete))
                   for single in q.parts for c in single.clauses)

    async def run_async(self, query: str, params: dict | None = None, **kw) -> Result:
        """Like run(), for asyncio code: the query queues FIFO for the graph lock without blocking
        the event loop, then executes in a worker thread. Writers run one at a time."""
        q = parse(query)
        lk = lock_of(self.graph)
        if q.explain or not lk.enabled:
            return await asyncio.get_running_loop().run_in_executor(
                None, lambda: self.run(query, params, **kw))
        async with lk.write_async() if self.is_write(q) else lk.read_async() as hold:
            return await lk.run_in_thread(hold, self.run, query, params, **kw)

    # -- running -------------------------------------------------------------- #
    def rebuild_id_index(self) -> dict:
        """Store mode: rebuild the reverse id table from graph-tool (write lock, O(V+E))."""
        if self.schema._id_mode != "store":
            raise OpenCypherError("rebuild_id_index() needs OpenCypher(..., id_index='store')")
        with self.lock.write():
            return self.schema.rebuild_reverse(self.graph, self.store)

    async def run_once_async(self, request_id: str, query: str, params: dict | None = None):
        """run_once() for asyncio code (see run_async)."""
        lk = lock_of(self.graph)
        if not lk.enabled or not self.is_write(query):
            return await asyncio.get_running_loop().run_in_executor(
                None, lambda: self.run_once(request_id, query, params))
        async with lk.write_async() as hold:
            return await lk.run_in_thread(hold, self.run_once, request_id, query, params)

    def run(self, query: str, params: dict | None = None, **kw) -> Result:
        """Run a query. Writing queries take the graph lock exclusively (queued FIFO behind
        earlier callers); reading queries share it. Held until staged writes are flushed."""
        p = {**(params or {}), **kw}
        q = parse(query)
        if q.explain:
            return Result(["plan"], [{"plan": l} for l in self.explain(query, p)], Counter())
        lk = lock_of(self.graph)
        hold = lk.acquire(self.is_write(q))
        try:
            return self._run_query(q, p)
        finally:
            lk.release(hold)

    def _run_query(self, q: "Query", p: dict) -> Result:
        runner = self.runner(self, p)
        g, store = self.graph, self.store
        if store is not None and not store.cache_across_queries:
            store.clear_cache()
        txn = None
        if self.atomic and self.is_write(q):
            if store is not None and store.unresolved:
                store.retry_unresolved()             # heal leftovers of an earlier failed write first
            txn = _Txn(g, store)                     # exclusive lock is held: invisible to others
            g.__dict__[_TXN_ATTR] = txn
            if store is not None:
                store._txn = txn
        try:
            parts = []
            for single in q.parts:
                cols, rows = runner.run_single(single)
                parts.append((cols, runner.finish(cols, rows) if cols else []))
            cols = parts[0][0]
            for c, _ in parts[1:]:
                if c != cols:
                    raise OpenCypherError("All sub queries in an UNION must have the same return column names")
            rows = [r for _, rs in parts for r in rs]
            if len(parts) > 1 and not q.all:
                seen, uniq = set(), []
                for r in rows:
                    k = tuple(_hk(r[c]) if not isinstance(r[c], (Node, Rel, Path)) else
                              (getattr(r[c], "id", id(r[c])),) for c in cols)
                    if k not in seen:
                        seen.add(k); uniq.append(r)
                rows = uniq
            if txn is not None and store is not None:
                store.flush(strict=True)             # the commit point (validates whole rows)
            rid = self._request_id if txn is not None else None
            ops = txn.redo + [["rq", rid, dict(runner.stats)]] if rid else (txn.redo if txn is not None else [])
            if ops and self.on_commit is not None:
                self.on_commit(ops)                  # e.g. a durable journal; raising rolls back
            if rid:
                self.remember_request(rid, dict(runner.stats))
            if txn is not None and store is not None:
                store.supersede(txn)
        except BaseException as ex:
            if txn is not None:
                self._rollback(txn, ex)
            raise
        finally:
            if txn is None:
                if store is not None:
                    store.flush()
            else:
                g.__dict__.pop(_TXN_ATTR, None)
                if store is not None:
                    store._txn = None
        return Result(cols, rows, runner.stats)

    def run_once(self, request_id: str, query: str, params: dict | None = None) -> tuple[Result | None, dict | None]:
        """Run a write at most once per `request_id` -> (result, None), or (None, the first
        run's {"stats"}) when that id has already committed. Checked under the write lock, so
        two concurrent calls with one id can't both run. Reads simply run."""
        q = parse(query)
        if not self.is_write(q):
            return self.run(query, params), None
        with self.lock.write():
            seen = self.request_ids.get(request_id)
            if seen is not None:
                return None, seen
            self._request_id = request_id
            try:
                return self.run(query, params), None
            finally:
                self._request_id = None

    def remember_request(self, request_id: str, stats: dict) -> None:
        self.request_ids[request_id] = {"stats": stats}
        self.request_ids.move_to_end(request_id)
        while len(self.request_ids) > self.request_id_retain:
            self.request_ids.popitem(last=False)

    # -- bulk import (no openCypher: rows straight in, see bulk_node_id) ---------------- #
    def _write_txn(self, work: Callable[[], Counter]) -> Counter:
        """Run `work()` as ONE write transaction, exactly like a write query: staged rows flushed
        and validated at the end, the topology journaled (on_commit), everything rolled back on
        failure. The caller holds the write lock."""
        g, store = self.graph, self.store
        if store is None or not self.atomic:
            raise OpenCypherError("bulk import needs a property store and atomic=True")
        if not store.cache_across_queries:
            store.clear_cache()
        if store.unresolved:
            store.retry_unresolved()
        txn = _Txn(g, store)
        g.__dict__[_TXN_ATTR] = txn
        store._txn = txn
        try:
            stats = work()
            store.flush(strict=True)
            rid = self._request_id
            ops = txn.redo + [["rq", rid, dict(stats)]] if rid else txn.redo
            if ops and self.on_commit is not None:
                self.on_commit(ops)
            if rid:
                self.remember_request(rid, dict(stats))
            store.supersede(txn)
            return stats
        except BaseException as ex:
            self._rollback(txn, ex)
            raise
        finally:
            g.__dict__.pop(_TXN_ATTR, None)
            store._txn = None

    def bulk_once(self, request_id: str | None, work: Callable[[], Counter]) -> tuple[Counter | None, dict | None]:
        """A bulk call at most once per request_id -> (stats, None) or (None, the first run's)."""
        with self.lock.write():
            if request_id is not None:
                seen = self.request_ids.get(request_id)
                if seen is not None:
                    return None, seen
            self._request_id = request_id
            try:
                return work(), None
            finally:
                self._request_id = None

    def _key_value(self, type_name: str, key: str, value: Any) -> Any:
        """The key as the data model stores it ("7" from a CSV and 7 from JSON are one node)."""
        if value is None or value == "":
            raise OpenCypherError(f"missing key {key!r}")
        try:
            return self.store.coerce_props("vertex", type_name, {key: value})[key]
        except (KeyError, ValueError) as ex:
            raise OpenCypherError(_msg(ex)) from None

    def _check_new_row(self, kind: str, type_name: str, props: dict) -> None:
        """errors="collect": what the commit would refuse about this row, found before it is applied
        (values; and for a new element, required fields and validators)."""
        store = self.store
        store.coerce_props(kind, type_name, props)
        if store.models is not None:
            store.models.validate_row(kind, type_name, props)

    @staticmethod
    def _reject(stats: Counter, n: int, kind: str, ex: BaseException) -> None:
        stats.setdefault("rejected", []).append(
            {"row": n, "kind": kind, "error": _msg(ex) if not isinstance(ex, OpenCypherError) else str(ex)})
        stats["rows_rejected"] += 1

    def bulk_nodes(self, type_name: str, key: str, rows: list[dict], labels: Iterable[str] = (),
                   on_existing: str = "update", errors: str = "stop") -> Counter:
        """Create or update one node per row; `key` is its natural key (a property of the row).
        on_existing: "update" (set the row's properties), "skip" (leave it) or "error".
        errors: "stop" (a bad row fails the whole call) or "collect" (bad rows are left out and
        listed in stats["rejected"]: [{"row": n, "kind": "invalid" | "duplicate", "error": ...}];
        the rest is applied)."""
        if on_existing not in ("update", "skip", "error"):
            raise OpenCypherError("on_existing must be update, skip or error")
        if errors not in ("stop", "collect"):
            raise OpenCypherError("errors must be stop or collect")
        labels = tuple(labels) or (type_name,)
        if type_name not in labels:
            labels = (type_name,) + labels
        g, sc = self.graph, self.schema
        collect = errors == "collect"

        def work() -> Counter:
            stats: Counter = Counter()
            seen: set = set()
            for n, row in enumerate(rows, 1):
                applying, why = False, "invalid"
                try:
                    props = {k: v for k, v in row.items() if v is not None and v != ""}
                    kv = self._key_value(type_name, key, row.get(key))
                    props[key] = kv
                    id_ = bulk_node_id(type_name, key, kv)
                    if id_ in seen:
                        why = "duplicate"
                        raise OpenCypherError(f"{key}={kv!r} appears twice in this batch")
                    v = sc.vertex_by_id(g, id_)
                    if v is not None and on_existing == "error":
                        raise OpenCypherError(f"{type_name} {key}={kv!r} already exists")
                    if collect and v is None:
                        self._check_new_row("vertex", type_name, props)
                    elif collect:
                        self.store.coerce_props("vertex", type_name, props)
                    seen.add(id_)
                    if v is not None:
                        if on_existing == "skip":
                            stats["nodes_skipped"] += 1
                            continue
                        stats["nodes_updated"] += 1
                    applying = True
                    if v is None:
                        v = _create_vertex(g, labels, type_name, id_)
                        stats["nodes_created"] += 1
                    _set_props(g, v, props)
                    stats["properties_set"] += len(props)
                except (OpenCypherError, KeyError, ValueError) as ex:
                    if collect and not applying:
                        self._reject(stats, n, why, ex)
                        continue
                    raise OpenCypherError(f"row {n}: {_msg(ex) if not isinstance(ex, OpenCypherError) else ex}") from None
            return stats
        return self._write_txn(work)

    def bulk_relationships(self, type_name: str, from_type: str, from_key: str, to_type: str, to_key: str,
                           rows: list[dict], on_missing: str = "error", on_existing: str = "update",
                           errors: str = "stop") -> Counter:
        """One relationship per row, from the node whose `from_key` is row["from"] to the node whose
        `to_key` is row["to"]; the row's other columns are its properties. At most one relationship
        of a type between two nodes (its id comes from the pair). Endpoints are found by their
        natural-key id; nodes created otherwise (openCypher) by an index search on the key.
        on_missing: "error" or "skip" (a row whose endpoint doesn't exist).
        errors: "stop" or "collect", as for bulk_nodes; a missing endpoint (with on_missing="error")
        is rejected with kind "missing_endpoint"."""
        if on_missing not in ("error", "skip") or on_existing not in ("update", "skip", "error"):
            raise OpenCypherError("on_missing must be error or skip; on_existing update, skip or error")
        if errors not in ("stop", "collect"):
            raise OpenCypherError("errors must be stop or collect")
        g, sc, store = self.graph, self.schema, self.store
        collect = errors == "collect"

        def endpoints(t: str, k: str, values: list) -> dict:
            """value -> vertex, by natural-key id, else one batched index search."""
            out, missing = {}, []
            for kv in values:
                v = sc.vertex_by_id(g, bulk_node_id(t, k, kv))
                if v is not None:
                    out[kv] = v
                else:
                    missing.append(kv)
            if missing:
                found = store.search("vertex", t, k, missing) or []    # None: the key isn't indexed
                for id_, r in found:
                    v = sc.vertex_by_id(g, id_)
                    if v is not None and r.get(k) in missing:
                        out.setdefault(r.get(k), v)
            return out

        def work() -> Counter:
            stats: Counter = Counter()
            pairs = []
            for n, row in enumerate(rows, 1):
                try:
                    pairs.append((self._key_value(from_type, from_key, row.get("from")),
                                  self._key_value(to_type, to_key, row.get("to"))))
                except OpenCypherError as ex:
                    if not collect:
                        raise OpenCypherError(f"row {n}: {ex}") from None
                    self._reject(stats, n, "invalid", ex)
                    pairs.append(None)
            src = endpoints(from_type, from_key, list({p[0] for p in pairs if p is not None}))
            dst = endpoints(to_type, to_key, list({p[1] for p in pairs if p is not None}))
            seen: set = set()
            for n, (row, pair) in enumerate(zip(rows, pairs), 1):
                if pair is None:
                    continue
                a, b = pair
                applying, why = False, "invalid"
                try:
                    va, vb = src.get(a), dst.get(b)
                    if va is None or vb is None:
                        what = f"{from_type} {from_key}={a!r}" if va is None else f"{to_type} {to_key}={b!r}"
                        if on_missing == "skip":
                            stats["relationships_skipped"] += 1
                            continue
                        why = "missing_endpoint"
                        raise OpenCypherError(f"no {what}")
                    id_ = stable_id("bulk", "edge", type_name, sc.get_id(g, va), sc.get_id(g, vb))
                    if id_ in seen:
                        why = "duplicate"
                        raise OpenCypherError(f"the pair ({a!r}, {b!r}) appears twice in this batch")
                    props = {k: v for k, v in row.items() if k not in ("from", "to") and v is not None and v != ""}
                    e = sc.edge_by_id(id_)
                    if e is not None and on_existing == "error":
                        raise OpenCypherError(f"{type_name} ({a!r})->({b!r}) already exists")
                    if collect and e is None:
                        self._check_new_row("edge", type_name, props)
                    elif collect:
                        store.coerce_props("edge", type_name, props)
                    seen.add(id_)
                    if e is not None:
                        if on_existing == "skip":
                            stats["relationships_skipped"] += 1
                            continue
                        stats["relationships_updated"] += 1
                    applying = True
                    if e is None:
                        e = _create_edge(g, va, vb, type_name, type_name, id_)
                        stats["relationships_created"] += 1
                    if props:
                        _set_props(g, e, props)
                        stats["properties_set"] += len(props)
                except (OpenCypherError, KeyError, ValueError) as ex:
                    if collect and not applying:
                        self._reject(stats, n, why, ex)
                        continue
                    raise OpenCypherError(f"row {n}: {_msg(ex) if not isinstance(ex, OpenCypherError) else ex}") from None
            return stats
        return self._write_txn(work)

    def _rollback(self, txn: "_Txn", cause: BaseException) -> None:
        """Undo a failed write query: graph-tool first (journal, newest first), then the store
        (compensation). Raises ConsistencyError if the store part could not be completed."""
        log.warning("write query failed, rolling back: %r", cause)
        graph_errs = []
        for fn in reversed(txn.undo):
            try:
                fn()
            except Exception as ex:                  # keep undoing the rest
                graph_errs.append(ex)
        self.schema.computed.clear()
        failures = self.store.rollback_txn(txn) if self.store is not None else []
        definite = [f for f in failures if f.get("definite")]
        if graph_errs or definite:
            log.error("rollback incomplete: %s graph undo error(s), %s store row(s) not put back "
                      "(run repair once the store is reachable)", len(graph_errs), len(definite))
            raise ConsistencyError(
                f"write failed ({cause!r}) and the rollback is incomplete: "
                f"{len(graph_errs)} graph undo error(s), {len(definite)} written store row(s) could "
                f"not be put back. Call cy.repair() once the store is reachable.",
                original=cause, unresolved=failures) from cause

    # -- consistency between graph-tool and the store ------------------------------------- #
    def verify(self, types: Iterable[str] | None = None, reverse: bool = True) -> "VerifyReport":
        """Compare the ids in graph-tool with the ids in every store table, both directions:
        `missing_rows` (in graph, no row) and `orphan_rows` (row, not in graph). Takes the read
        lock; costs one full id scan per table. Also lists corrections still pending from a
        failed rollback (`unresolved`). `types` limits it to those tables, `reverse=False` skips
        the reverse id table (store mode), so a check can go one table at a time."""
        with self.lock.read():
            return self._verify(None if types is None else set(types), reverse)

    def _verify(self, types: "set | None", reverse: bool = True) -> "VerifyReport":
        g, sc, store = self.graph, self.schema, self.store
        if store is None:
            raise OpenCypherError("verify() needs a property store")
        rep = VerifyReport()
        use_rev = sc._id_mode == "store"
        expected: dict = {}                       # id -> expected reverse row (store mode)
        for kind in ("vertex", "edge"):
            by_type: dict = {}
            pm = (g.vp if kind == "vertex" else g.ep)[sc.id_map]
            for el in (g.vertices() if kind == "vertex" else g.edges()):
                raw = pm[el]
                if not raw:
                    continue                      # retired / tombstoned
                id_ = norm_id(raw)
                by_type.setdefault(sc.type_name(g, el), set()).add(id_)
                if use_rev:
                    expected[id_] = ({"kind": "v", "idx": int(el)} if kind == "vertex" else
                                     {"kind": "e", "idx": int(g.edge_index[el]),
                                      "src": int(el.source()), "dst": int(el.target())})
            names = set(by_type) | set(store._types.get(kind) or ())
            for tn in sorted(t for t in names if t is not None and (types is None or t in types)):
                gids = by_type.get(tn, set())
                try:
                    sids = {_norm_or_raw(x) for x in store.scan_ids(kind, tn)}
                except KeyError:
                    if gids:
                        rep.missing_tables.append((kind, tn))
                    continue
                rep.checked[(kind, tn)] = (len(gids), len(sids))
                if gids - sids:
                    rep.missing_rows[(kind, tn)] = sorted(gids - sids)
                if sids - gids:
                    rep.orphan_rows[(kind, tn)] = sorted(sids - gids)
        if use_rev and reverse:
            self._verify_reverse(rep, expected)
        rep.unresolved = list(store.unresolved)
        return rep

    def _verify_reverse(self, rep: "VerifyReport", expected: dict) -> None:
        store = self.store
        rev = {_norm_or_raw(x) for x in store.scan_ids("reverse", "ids")}
        rep.reverse_marker = _REV_MARKER in rev
        rev.discard(_REV_MARKER)
        rep.reverse_missing = sorted(expected.keys() - rev)
        rep.reverse_orphans = sorted(rev - expected.keys())
        both = sorted(expected.keys() & rev)
        for k in range(0, len(both), 500):
            chunk = both[k:k + 500]
            rows = store.fetch_rows("reverse", "ids", chunk)
            for i in chunk:
                r = rows.get(i) or {}
                if any(r.get(c) != v for c, v in expected[i].items()):
                    rep.reverse_stale.append(i)

    def repair(self, missing: str = "report", orphans: str = "report",
               retry_journal: bool = True, reverse: str = "report") -> dict:
        """Bring graph-tool and the store back in line. Takes the write lock.

        1. `retry_journal`: re-apply corrections that failed in an earlier rollback.
        2. verify(), then, per policy:
             missing  (graph element without a row):
                "report"    do nothing (default)
                "recreate"  write an empty row (the id is back in sync, its properties are lost)
                "tombstone" remove the element from the graph (vertices are tombstoned, their
                            relationships removed)
             orphans  (row whose id is not in the graph):
                "report"    do nothing (default)
                "delete"    delete the rows
             reverse  (id_index="store": the reverse id table vs graph-tool; graph-tool wins):
                "report"    do nothing (default)
                "rebuild"   rewrite every row from the graph, delete orphans, set the marker
        Returns counts plus the `before` / `after` VerifyReports. Not atomic: a failure midway
        can simply be repaired again."""
        if missing not in ("report", "recreate", "tombstone"):
            raise ValueError("missing must be 'report', 'recreate' or 'tombstone'")
        if orphans not in ("report", "delete"):
            raise ValueError("orphans must be 'report' or 'delete'")
        if reverse not in ("report", "rebuild"):
            raise ValueError("reverse must be 'report' or 'rebuild'")
        g, sc, store = self.graph, self.schema, self.store
        if store is None:
            raise OpenCypherError("repair() needs a property store")
        out: dict = {"journal_retried": 0, "journal_still_failing": 0, "rows_recreated": 0,
                     "graph_elements_removed": 0, "orphan_rows_deleted": 0, "reverse_rebuilt": None}
        with self.lock.write():
            if retry_journal:
                out["journal_retried"], out["journal_still_failing"] = store.retry_unresolved()
            before = self._verify(None)
            if missing == "recreate":
                for (kind, tn), ids in before.missing_rows.items():
                    for k in range(0, len(ids), 500):
                        store.write_rows(kind, tn, [(i, {}) for i in ids[k:k + 500]])
                    out["rows_recreated"] += len(ids)
            elif missing == "tombstone":
                verts, edges = {}, {}
                for (kind, tn), ids in before.missing_rows.items():
                    for i in ids:
                        if kind == "vertex":
                            verts[i] = g.vertex(sc._vidx[i])
                        else:
                            edges[i] = sc._eidx[i]
                seen = {int(g.edge_index[e]) for e in edges.values()}
                for v in verts.values():             # detach: remove incident relationships too
                    for e in list(v.all_edges()):
                        if int(g.edge_index[e]) not in seen:
                            seen.add(int(g.edge_index[e]))
                            edges[sc.get_id(g, e) or f"#{int(g.edge_index[e])}"] = e
                _delete_elements(g, sc, store, verts, edges)
                store.flush()
                out["graph_elements_removed"] = len(verts) + len(edges)
            if orphans == "delete":
                for (kind, tn), ids in before.orphan_rows.items():
                    for k in range(0, len(ids), 500):
                        store.delete_rows(kind, tn, ids[k:k + 500])
                    out["orphan_rows_deleted"] += len(ids)
            if reverse == "rebuild" and sc._id_mode == "store":
                out["reverse_rebuilt"] = sc.rebuild_reverse(g, store)
            store.clear_cache()
            out["before"], out["after"] = before, self._verify(None)
        return out

    def explain(self, query: str, params: dict | None = None) -> list[str]:
        """How the query will be executed: start nodes, how seeds are found, pushdown."""
        q = parse(query)
        run = self.runner(self, params or {})
        lines: list[str] = []
        for si, single in enumerate(q.parts):
            if si:
                lines.append("UNION" + (" ALL" if q.all else ""))
            scope: set = set()
            for c in single.clauses:
                if isinstance(c, Match):
                    try:
                        hints = run.where_hints(c.where) if c.where is not None else {}
                    except OpenCypherError:
                        hints = {}
                    for pi, part in enumerate(c.parts):
                        lines += self._explain_part(run, part, scope, hints,
                                                    ("OPTIONAL " if c.optional else "") + "MATCH")
                        scope |= set(_pattern_vars([part]))
                    if c.where is not None:
                        lines.append("  filter: WHERE (evaluated after matching; "
                                     + ("equality/label terms pushed into the start node)" if hints else "no pushdown)"))
                elif isinstance(c, Proj):
                    agg = any(_has_agg(i.expr) for i in c.items)
                    how = ("aggregate (barrier)" if agg else
                           "sort (barrier)" if c.order else
                           "stream" + (" -- LIMIT stops early" if c.limit is not None else ""))
                    lines.append(f"{c.kind.upper()}: {how}" + (" + DISTINCT" if c.distinct else ""))
                    scope = {i.alias for i in c.items} | (scope if c.star else set())
                elif isinstance(c, Unwind):
                    lines.append("UNWIND"); scope.add(c.var)
                elif isinstance(c, (Create, Merge, SetC, Remove, Delete)):
                    lines.append(type(c).__name__.upper() + ": eager (reads finish before writes; store writes are staged and flushed at the end)")
                    if isinstance(c, Create): scope |= set(_pattern_vars(c.parts))
                    if isinstance(c, Merge): scope |= set(_pattern_vars([c.part]))
                elif isinstance(c, Call):
                    lines.append(f"CALL {c.name}")
        return lines

    def _explain_part(self, run, part, scope, hints, what) -> list[str]:
        s = run.choose_start(part, scope, hints)
        node = part.nodes[s]
        def fmt(n):
            return "(" + (n.var or "") + "".join(":" + l for l in n.labels) + ")"
        if node.var is not None and node.var in scope:
            how = "already bound"
        else:
            h = hints.get(node.var) if node.var else None
            try:
                labels, eqs = run.node_cons(node, {}, h)
                kind, detail = run.seed_how(labels, eqs)
            except OpenCypherError:
                kind, detail = "scan", None
            how = {"id": "uuid lookup", "store": f"store search on {detail!r} (falls back to a scan if not searchable)",
                   "mask": f"label bit-scan {list(detail) if detail else ''}", "scan": "full vertex scan"}[kind]
        out = [f"{what}: start at {fmt(node)} via {how}"]
        for i, rel in enumerate(part.rels):
            arrow = {"out": "-[{}]->", "in": "<-[{}]-", "both": "-[{}]-"}[rel.direction]
            body = (rel.var or "") + ("".join((":" if j == 0 else "|") + t for j, t in enumerate(rel.types)))
            if rel.varlen:
                body += f"*{rel.lo}..{'' if rel.hi is None else rel.hi}"
            out.append(f"  expand {fmt(part.nodes[i])}{arrow.format(body)}{fmt(part.nodes[i + 1])}"
                       + ("  [shortest path BFS]" if part.mode else ""))
        return out


def opencypher(graph: Graph, store: "PropertyStore | None" = None, schema: Schema | None = None,
           varlen_cap: int = 64, lock: bool = True, atomic: bool = True,
           stable_merge_ids: bool = True, id_index: str | None = None,
           models: "Models | None" = None, migrate: bool = True) -> OpenCypher:
    """cy = opencypher(graph[, store][, schema]); see OpenCypher.
    lock=False disables the per-graph readers-writer lock."""
    log.debug("openCypher engine over a graph of %s vertices and %s edges (store %s, id index %s)",
              graph.num_vertices(), graph.num_edges(), type(store).__name__ if store is not None else "none",
              id_index or "memory")
    return OpenCypher(graph, store, schema, varlen_cap, lock, atomic, stable_merge_ids, id_index,
                  models, migrate)
