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

"""Property stores: the interface (PropertyStore) and an in-memory one (InMemoryStore)."""
from __future__ import annotations

import logging
from collections import Counter
from graph_tool import Graph
from typing import Any
from typing import Iterable
from .errors import OpenCypherError
from .common import _CHUNKS, _Txn
from .values import _refuse_maps
from .models import Models
from .schema import _schema
from .elements import _is_e, _is_v, _prop

log = logging.getLogger(__name__)

class PropertyStore:
    """Base class: row cache + batching. Subclasses implement the 3 hooks below.

    Hooks:
      columns(kind, type_name)                  -> list of column names
      fetch_rows(kind, type_name, ids)          -> {id: {column: value}}
      search(kind, type_name, key, values)      -> [(id, row)] or None if the
                                                   column isn't searchable
    `kind` is "vertex" or "edge".
    """

    def __init__(self, vertex_types: Iterable[str] | None = None,
                 edge_types: Iterable[str] | None = None, max_cache: int = 200_000):
        self._types: dict[str, set | None] = {
            "vertex": set(vertex_types) if vertex_types else None,
            "edge": set(edge_types) if edge_types else None,
        }
        self._cache: dict[tuple, dict] = {}
        self.max_cache = max_cache
        # False: forget cached rows at the start of every query. Needed when someone else writes
        # the tables (e.g. a read replica of another process's graph); True is right when this
        # process is the only writer, because its own writes keep the cache current.
        self.cache_across_queries = True
        self._pending_w: dict[tuple, dict] = {}   # (kind, type, id) -> staged props
        self._pending_del: set[tuple] = set()      # (kind, type, id)
        self.models: "Models | None" = None        # pydantic models: schema, DDL, validation
        self._txn: "_Txn | None" = None            # set by `OpenCypher` while a write query runs
        self.unresolved: list[dict] = []           # failed compensations (see retry_unresolved)
        # --- tuning for filters that run AFTER navigation (see _FilterStep) ---
        self.chunk_sizes: tuple = _CHUNKS          # growing prefetch chunk sizes
        self.semijoin_min: int = 1000              # use an id-only index search once a
                                                   # stream has produced this many elements
        self.semijoin_limit: int = 50_000          # ...and give up if it matches more ids
        self.stats: Counter = Counter()            # semijoins / semijoin_fallbacks

    # -- lifecycle ---------------------------------------------------------- #
    id_column = "id"

    def attach_models(self, models: "Models") -> None:
        """Adopt the pydantic models as the schema: types come from them (and, for Cassandra,
        table names too); values are validated when set and rows at commit."""
        self.models = models
        self._types = {"vertex": set(models.type_names("vertex")),
                       "edge": set(models.type_names("edge"))}

    def migrate(self, models: "Models", reverse: bool = False, allow_blocked: bool = False):
        """Create/alter the backing tables for `models`. No-op unless the store has tables to manage."""
        return None

    def coerce_props(self, kind: str, type_name: str, props: dict) -> dict:
        """Validate/convert property values being set (without models: maps are refused)."""
        if self.models is None:
            _refuse_maps(props, lambda k: False)
            return props
        return self.models.coerce_props(kind, type_name, props)

    def bind(self, g: Graph) -> None:
        """Learn which types exist (from the Schema's type registries)."""
        sc = _schema(g)
        for kind in ("vertex", "edge"):
            if self._types[kind] is None:
                self._types[kind] = set(sc.reg(kind, "type").all_names())

    def clear_cache(self) -> None:
        self._cache.clear()

    # -- hooks -------------------------------------------------------------- #
    def columns(self, kind: str, type_name: str) -> list[str]:
        raise NotImplementedError

    def fetch_rows(self, kind: str, type_name: str, ids: list[int]) -> dict[int, dict]:
        raise NotImplementedError

    def search(self, kind: str, type_name: str, key: str, values: list):
        return None

    def vector_search(self, kind: str, type_name: str, field: str, vector: list, k: int,
                      filters: dict, similarity: str) -> list:
        """The `k` rows whose `field` is most similar to `vector` (and whose columns equal
        `filters`), best first: [(id, score)], score scaled to 0..1 like Cassandra's."""
        raise OpenCypherError(f"{type(self).__name__} has no vector search")

    def semijoin_ids(self, kind: str, type_name: str, key: str, values: list, limit: int):
        """IDs (str) of every row of this table whose `key` is in `values`, for use
        as a semi-join against a large stream. Returns None when the column isn't
        searchable cheaply or more than `limit` rows match (caller falls back to
        fetching rows). Default: derive from search(); stores should override with
        an id-only query."""
        try:
            cols = self.columns(kind, type_name)
        except KeyError:
            return set()
        if key not in cols:
            return set()
        rows = self.search(kind, type_name, key, values)
        if rows is None or len(rows) > limit:
            return None
        return {i for i, _ in rows}

    # write hooks (optional; a store without them is read-only)
    def write_rows(self, kind: str, type_name: str, rows: list[tuple[int, dict]]) -> None:
        raise NotImplementedError(f"{type(self).__name__} is read-only")

    def delete_rows(self, kind: str, type_name: str, ids: list[int]) -> None:
        raise NotImplementedError(f"{type(self).__name__} is read-only")

    def scan_ids(self, kind: str, type_name: str):
        """Every id stored in this type's table (used by OpenCypher.verify/repair). Raise KeyError
        if the table does not exist."""
        raise NotImplementedError(f"{type(self).__name__} cannot list ids")

    def scan_rows(self, kind: str, type_name: str):
        """Every row of this type's table as (id, row) with the id column removed and null
        columns left out, in one paged pass (backups). KeyError if the table does not exist."""
        raise NotImplementedError(f"{type(self).__name__} cannot scan rows")

    def truncate(self, kind: str, type_name: str) -> None:
        """Delete every row of this type's table (restores). KeyError if it does not exist."""
        raise NotImplementedError(f"{type(self).__name__} cannot truncate")

    def table_columns(self, kind: str, type_name: str) -> "set[str] | None":
        """The columns this type's table has, or None when any column is accepted."""
        return None

    # validation hooks: raise early (before graph-tool is mutated)
    def validate_type(self, kind: str, type_name: str) -> None: ...
    def validate_columns(self, kind: str, type_name: str, cols: list[str]) -> None: ...

    # -- cached access ------------------------------------------------------ #
    def get(self, kind: str, type_name: str, id_: int, key: str) -> Any:
        ck = (kind, type_name, id_)
        if ck not in self._cache:
            self._load(kind, type_name, [id_])
        return self._cache[ck].get(key)

    def row(self, kind: str, type_name: str, id_: Any) -> dict:
        """The element's whole (cached, staged-write-aware) row."""
        ck = (kind, type_name, id_)
        if ck not in self._cache:
            self._load(kind, type_name, [id_])
        return self._cache[ck]

    def prefetch(self, g: Graph, elements: Iterable[Any]) -> None:
        """Load rows for many elements: one batch per table, skipping cached ones."""
        need: dict[tuple, list] = {}
        for el in elements:
            is_v = _is_v(el)
            if not (is_v or _is_e(el)):
                continue
            kind = "vertex" if is_v else "edge"
            tn = _prop(g, el, "type")
            if not tn:
                continue
            id_ = _prop(g, el, "id")
            if id_ is None:
                continue
            if (kind, tn, id_) not in self._cache:
                need.setdefault((kind, tn), []).append(id_)
        for (kind, tn), ids in need.items():
            self._load(kind, tn, ids)

    def prefetch_keys(self, kind: str, tn: str, ids: Iterable) -> None:
        """Batch-load rows by key (one round trip per call), skipping cached ones."""
        need = [i for i in dict.fromkeys(ids) if (kind, tn, i) not in self._cache]
        if need:
            self._load(kind, tn, need)

    def _load(self, kind: str, tn: str, ids: list[int]) -> None:
        if len(self._cache) + len(ids) > self.max_cache:
            self._cache.clear()
        rows = self.fetch_rows(kind, tn, ids)
        for i in ids:
            ck = (kind, tn, i)
            # overlay staged (unflushed) changes so evictions never lose them
            row = {} if ck in self._pending_del else dict(rows.get(i) or {})
            if ck in self._pending_w:
                row.update(self._pending_w[ck])
                row.setdefault("id", i)
            self._cache[ck] = row

    # -- staged writes ------------------------------------------------------ #
    def register_type(self, kind: str, type_name: str) -> None:
        if self._types.get(kind) is None:
            self._types[kind] = set()
        self._types[kind].add(type_name)

    def stage_write(self, kind: str, tn: str, id_: int, props: dict) -> None:
        """Queue an upsert of `props` (an empty dict just ensures the row exists)."""
        ck = (kind, tn, id_)
        self._pending_del.discard(ck)
        self._pending_w.setdefault(ck, {}).update(props)
        if ck in self._cache:
            self._cache[ck].update(props)
            self._cache[ck].setdefault("id", id_)
        self.register_type(kind, tn)

    def stage_delete(self, kind: str, tn: str, id_: int) -> None:
        ck = (kind, tn, id_)
        self._pending_w.pop(ck, None)
        self._pending_del.add(ck)
        self._cache[ck] = {}

    def has_pending(self) -> bool:
        return bool(self._pending_w or self._pending_del)

    def flush(self, strict: bool = False) -> None:
        """Write staged changes: deletes first, then upserts; batched per table.

        strict=True (the commit point of a write query, with models attached): every row this
        query touches is validated as a WHOLE against its pydantic model first (required fields,
        validators), defaults are filled in for new rows, and a violation raises ConstraintError
        before anything is written. Mid-query flushes are never strict.

        Idempotent: if anything raises, the pending buffer is kept so flush()
        can be called again. While a write query runs (`self._txn` set) the rows' pre-images are
        read first and every key sent is recorded, so a failure can be compensated
        (see rollback_txn); nothing is written if the pre-image reads already fail.
        """
        if not self.has_pending():
            return
        txn = self._txn
        dels: dict[tuple, list] = {}
        writes: dict[tuple, list] = {}
        for kind, tn, i in self._pending_del:
            dels.setdefault((kind, tn), []).append(i)
        for (kind, tn, i), props in self._pending_w.items():
            writes.setdefault((kind, tn), []).append((i, dict(props)))
        validate = txn is not None and strict and self.models is not None
        if validate:
            # rows an earlier (mid-query) flush already wrote are validated as a whole too
            for ck in txn.written:
                if ck not in self._pending_w and ck not in self._pending_del:
                    writes.setdefault(ck[:2], []).append((ck[2], {}))
        pending = set(self._pending_w)
        if txn is not None:
            self._capture_preimages(txn, dels, writes)
            if validate:
                self._strict_check(txn, writes)
                writes = {t: [(i, p) for i, p in rows if p or (*t, i) in pending]
                          for t, rows in writes.items()}
        for (kind, tn), ids in dels.items():
            ids = sorted(ids)
            if txn is not None:
                txn.attempted.update((kind, tn, i) for i in ids)
                for i in ids:
                    txn.written.pop((kind, tn, i), None)
            self.delete_rows(kind, tn, ids)
            if txn is not None:
                txn.applied.update((kind, tn, i) for i in ids)
        for (kind, tn), rows in writes.items():
            if not rows:
                continue
            rows = sorted(rows, key=lambda r: r[0])
            if txn is not None:
                for i, props in rows:
                    txn.attempted.add((kind, tn, i))
                    txn.cols.setdefault((kind, tn, i), set()).update(props)
                    txn.written.setdefault((kind, tn, i), {}).update(props)
            self.write_rows(kind, tn, rows)
            if txn is not None:
                txn.applied.update((kind, tn, i) for i, _ in rows)
        self._pending_del.clear()
        self._pending_w.clear()

    def _strict_check(self, txn: "_Txn", writes: dict) -> None:
        m = self.models
        for (kind, tn), rows in writes.items():
            info = m._by.get((kind, tn))
            if info is None:
                continue                          # not a model table (e.g. the reverse id table)
            for i, props in rows:
                ck = (kind, tn, i)
                full = {k: v for k, v in (txn.pre.get(ck) or {}).items()
                        if k != self.id_column and v is not None}
                for k, v in list(txn.written.get(ck, {}).items()) + list(props.items()):
                    if v is None:
                        full.pop(k, None)
                    else:
                        full[k] = v
                final = m.validate_row(kind, tn, full, f"{info.type} {i}")
                changes = {k: final[k] for k, v in props.items() if v is not None and final.get(k) != v}
                if ck in txn.created:             # defaults for properties nobody set
                    changes.update({k: v for k, v in final.items() if k not in full and v is not None})
                if changes:
                    props.update(changes)
                    self._pending_w.setdefault(ck, {}).update(changes)
                    if ck in self._cache:
                        self._cache[ck].update(changes)

    def _capture_preimages(self, txn: "_Txn", dels: dict, writes: dict) -> None:
        need: dict[tuple, set] = {}
        for (kind, tn), ids in dels.items():
            need.setdefault((kind, tn), set()).update(ids)
        for (kind, tn), rows in writes.items():
            need.setdefault((kind, tn), set()).update(i for i, _ in rows)
        for (kind, tn), ids in need.items():
            fetch = []
            for i in ids:
                ck = (kind, tn, i)
                if ck in txn.pre:
                    continue                     # first pre-image wins (earlier flush of this query)
                if ck in txn.created:
                    txn.pre[ck] = None
                else:
                    fetch.append(i)
            if fetch:
                rows = self.fetch_rows(kind, tn, sorted(fetch))
                for i in fetch:
                    r = rows.get(i)
                    txn.pre[(kind, tn, i)] = dict(r) if r else None

    def rollback_txn(self, txn: "_Txn") -> list:
        """Compensate a failed write query: drop staged changes, then put every row the query
        may have touched back to its pre-image (delete rows it created, restore the others).
        Best effort; whatever cannot be done now is appended to `self.unresolved` (and returned)
        so `retry_unresolved()` / `OpenCypher.repair()` can finish it later. A failure is `definite`
        when its row was certainly written (the store call returned); otherwise the failed call
        itself may or may not have applied anything (e.g. the store was simply down)."""
        for ck in list(self._pending_w) + list(self._pending_del):
            self._cache.pop(ck, None)
        self._pending_w.clear()
        self._pending_del.clear()
        dels: dict[tuple, list] = {}
        restores: dict[tuple, list] = {}
        for ck in txn.attempted:
            kind, tn, i = ck
            if ck not in txn.pre:
                continue
            pre = txn.pre[ck]
            if pre is None:
                dels.setdefault((kind, tn), []).append(i)
            else:
                row = {c: None for c in txn.cols.get(ck, ())}      # columns the query added
                row.update({k: v for k, v in pre.items() if k != self.id_column})
                restores.setdefault((kind, tn), []).append((i, row))
        failures: list[dict] = []
        for (kind, tn), ids in dels.items():
            try:
                self.delete_rows(kind, tn, sorted(ids))
            except Exception as ex:
                failures += [{"kind": kind, "type": tn, "id": i, "action": "delete", "row": None,
                              "error": repr(ex), "definite": (kind, tn, i) in txn.applied}
                             for i in ids]
        for (kind, tn), rows in restores.items():
            try:
                self.write_rows(kind, tn, sorted(rows, key=lambda r: r[0]))
            except Exception as ex:
                failures += [{"kind": kind, "type": tn, "id": i, "action": "restore", "row": r,
                              "error": repr(ex), "definite": (kind, tn, i) in txn.applied}
                             for i, r in rows]
        for ck in set(txn.attempted) | set(txn.created):
            self._cache.pop(ck, None)
        self.unresolved.extend(failures)
        return failures

    def supersede(self, txn: "_Txn") -> None:
        """A committed query rewrote these keys: older pending corrections for them are obsolete
        (with stable MERGE ids a retried MERGE lands here and simply wins)."""
        if self.unresolved:
            self.unresolved = [u for u in self.unresolved
                               if (u["kind"], u["type"], u["id"]) not in txn.attempted]

    def retry_unresolved(self) -> tuple[int, int]:
        """Retry corrections that failed during an earlier rollback. -> (done, still_failing)."""
        left, done = [], 0
        for ent in self.unresolved:
            try:
                if ent["action"] == "delete":
                    self.delete_rows(ent["kind"], ent["type"], [ent["id"]])
                else:
                    self.write_rows(ent["kind"], ent["type"], [(ent["id"], ent["row"])])
                self._cache.pop((ent["kind"], ent["type"], ent["id"]), None)
                done += 1
            except Exception as ex:
                ent["error"] = repr(ex)
                left.append(ent)
        self.unresolved = left
        if done or left:
            (log.warning if left else log.info)("rollback corrections retried: %s done, %s still failing", done, len(left))
        return done, len(left)

    def find_rows(self, kind: str, key: str, values: list, types: set | None = None):
        """Search every candidate table that has column `key`.

        Returns [(type_name, id, row)] or None if any candidate table can't
        search that column (caller must then fall back to a full scan).
        """
        out = []
        for tn in sorted(types if types is not None else (self._types[kind] or ())):
            try:
                cols = self.columns(kind, tn)
            except KeyError:
                continue  # a type without a table has no rows
            if key not in cols:
                continue  # this type never has the property -> no matches
            found = self.search(kind, tn, key, values)
            if found is None:
                return None
            for id_, row in found:
                self._cache[(kind, tn, id_)] = row
                out.append((tn, id_, row))
        return out


class InMemoryStore(PropertyStore):
    """Dict-backed store for tests/demos. tables = {(kind, type): {id: row}}.

    `indexed` = {(kind, type): {searchable columns}}; None means all columns.
    `fetches` / `searches` count round-trips, handy for checking batching.
    """

    def __init__(self, tables: dict, indexed: dict | None = None, **kw):
        super().__init__(**kw)
        self.tables, self.indexed = tables, indexed
        self.tables.setdefault(("reverse", "ids"), {})     # the id -> index table (see Schema)
        self.fetches = self.searches = 0
        self.write_calls = self.delete_calls = 0
        self.semijoin_queries = 0

    def semijoin_ids(self, kind, type_name, key, values, limit):
        if (kind, type_name) not in self.tables or key not in self.columns(kind, type_name):
            return set()
        if self.indexed is not None and key not in self.indexed.get((kind, type_name), ()):
            return None
        self.semijoin_queries += 1
        ids = {i for i, r in self.tables[(kind, type_name)].items() if r.get(key) in values}
        return ids if len(ids) <= limit else None

    def validate_type(self, kind, type_name):
        if (kind, type_name) not in self.tables:
            raise KeyError(f"No table for {kind} type {type_name!r}")

    def scan_ids(self, kind, type_name):
        self.validate_type(kind, type_name)
        return list(self.tables[(kind, type_name)])

    def scan_rows(self, kind, type_name):
        self.validate_type(kind, type_name)
        for i, row in list(self.tables[(kind, type_name)].items()):
            yield i, {k: v for k, v in row.items() if k != "id" and v is not None}

    def truncate(self, kind, type_name):
        self.validate_type(kind, type_name)
        self.tables[(kind, type_name)].clear()

    def attach_models(self, models):
        super().attach_models(models)
        if self.indexed is None:
            self.indexed = {}
        for (kind, tn), info in models._by.items():
            self.tables.setdefault((kind, tn), {})
            self.indexed[(kind, tn)] = {f for f, fi in info.fields.items() if fi.index}

    def write_rows(self, kind, type_name, rows):
        self.write_calls += 1
        t = self.tables[(kind, type_name)]
        for id_, props in rows:
            row = t.setdefault(id_, {"id": id_})
            for k, v in props.items():
                if v is None:
                    row.pop(k, None)     # null == absent, like Cassandra
                else:
                    row[k] = v

    def delete_rows(self, kind, type_name, ids):
        self.delete_calls += 1
        t = self.tables[(kind, type_name)]
        for i in ids:
            t.pop(i, None)

    def columns(self, kind, type_name):
        cols = {"id"}
        for row in self.tables[(kind, type_name)].values():
            cols.update(row)
        return sorted(cols)

    def fetch_rows(self, kind, type_name, ids):
        self.fetches += 1
        t = self.tables[(kind, type_name)]
        return {i: dict(t[i]) for i in ids if i in t}

    def search(self, kind, type_name, key, values):
        if self.indexed is not None and key not in self.indexed.get((kind, type_name), ()):
            return None
        self.searches += 1
        return [(i, dict(r)) for i, r in self.tables[(kind, type_name)].items()
                if r.get(key) in values]

    def vector_search(self, kind, type_name, field, vector, k, filters, similarity):
        """Exact (brute force) nearest neighbours: the reference the approximate Cassandra
        search is measured against."""
        self.searches += 1
        scored = []
        for i, r in self.tables.get((kind, type_name), {}).items():
            v = r.get(field)
            if v is None or any(r.get(c) != want for c, want in filters.items()):
                continue
            s = _vector_score(similarity, v, vector)
            if s is not None:
                scored.append((i, s))
        scored.sort(key=lambda x: -x[1])
        return scored[:k]


def _link() -> None:
    """Bind the names this module uses from modules loaded after it (they import this one).
    Called once by the package __init__, after every module is loaded."""
    global _vector_score
    from .graph_functions import _vector_score
