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

"""The Cassandra property store (needs cassandra-driver: the `cassandra` extra)."""
from __future__ import annotations

import logging
import time
import uuid as _uuid
import warnings
from typing import Any
from typing import Callable
from typing import Iterable
from .errors import OpenCypherError
from .ids import norm_id
from .models import Models, _index_details, _is_sai
from .schema import Schema
from .stores import PropertyStore

log = logging.getLogger(__name__)

REVERSE_ID_TABLE = "reverse_vertex_ids"   # default name of the id -> index table


class CassandraStore(PropertyStore):
    """Apache Cassandra backend (needs `pip install cassandra-driver`).

    One table per vertex type and per edge type, primary key = `id_column`.
    The graph-tool "type" value is mapped to a table name with `table_namer`:

        CassandraStore(session, "graph",
                       table_namer=lambda kind, t: ("v_" if kind == "vertex" else "e_") + t)

    Pushdown needs searchable columns. By default searchability is detected
    from table metadata (partition key + secondary indexes). Override with
    `searchable={"table": {"col", ...}}`, or set `allow_filtering=True` to
    allow ALLOW FILTERING queries on unindexed columns (can be very slow).

    `read_consistency` / `write_consistency` (cassandra.ConsistencyLevel values, e.g.
    ConsistencyLevel.LOCAL_QUORUM) are set on every prepared SELECT / write statement; None
    keeps the session's default. `concurrency` caps in-flight requests per batch.

    Tables with a vector index get at most `vector_write_concurrency` writes in flight: Cassandra
    5.0's vector indexes refuse more simultaneous writes than about one per CPU core ("Number of
    outstanding pooled objects has gone beyond the limit"). Writes it still refuses are retried
    (an INSERT is an upsert, so repeating it is safe).
    """

    def __init__(self, session, keyspace: str,
                 table_namer: Callable[[str, str], str] | None = None,
                 id_column: str = "id", allow_filtering: bool = False,
                 searchable: dict[str, set] | None = None,
                 reverse_table: str = REVERSE_ID_TABLE, id_type: str = "uuid",
                 read_consistency: Any = None, write_consistency: Any = None,
                 concurrency: int = 100, vector_write_concurrency: int = 8, **kw):
        super().__init__(**kw)
        self.read_consistency, self.write_consistency = read_consistency, write_consistency
        self.concurrency = concurrency
        self.vector_write_concurrency = max(1, vector_write_concurrency)
        self.reverse_table = reverse_table   # id -> graph-tool index (`OpenCypher(..., id_index="store")`)
        self.id_type = id_type               # CQL type of id columns in tables WE create
        self.session, self.keyspace = session, keyspace  # ids: text OR uuid columns both work
        self.table_namer = table_namer or (lambda kind, type_name: type_name)
        self.id_column = id_column
        self.allow_filtering = allow_filtering
        self.searchable = searchable
        self._stmts: dict[str, Any] = {}

    def attach_models(self, models: "Models") -> None:
        super().attach_models(models)
        self.table_namer = models.table_namer           # table names come from the models

    def migrate(self, models: "Models", reverse: bool = False, allow_blocked: bool = False):
        return models.apply(self, reverse=reverse, allow_blocked=allow_blocked)

    def reverse_table_ddl(self, id_type: str | None = None) -> str:
        """CQL for the reverse id table (id = uuid or text, like your other tables)."""
        id_type = id_type or self.id_type
        return (f'CREATE TABLE IF NOT EXISTS "{self.keyspace}"."{self.reverse_table}" '
                f'("{self.id_column}" {id_type} PRIMARY KEY, kind text, idx bigint, src bigint, dst bigint)')

    def create_reverse_table(self, id_type: str | None = None) -> None:
        self.session.execute(self.reverse_table_ddl(id_type))

    def _meta(self, kind: str, type_name: str):
        table = self.reverse_table if kind == "reverse" else self.table_namer(kind, type_name)
        tables = self.session.cluster.metadata.keyspaces[self.keyspace].tables
        if table not in tables:  # also guarantees `table` is a real identifier
            raise KeyError(f"No Cassandra table {table!r} for {kind} type {type_name!r}")
        return table, tables[table]

    def _prepared(self, cql: str):
        if cql not in self._stmts:
            stmt = self.session.prepare(cql)
            cl = self.read_consistency if cql.startswith("SELECT") else self.write_consistency
            if cl is not None:
                stmt.consistency_level = cl
            self._stmts[cql] = stmt
        return self._stmts[cql]

    def _concurrent(self, stmt, params, concurrency: int | None = None):
        """[(ok, result or exception)] per statement: every caller looks at each one (the driver's
        default, raise_on_first_error=True, would raise at the first failure instead, and the
        vector write retries in _write_all would never run)."""
        from cassandra.concurrent import execute_concurrent_with_args
        return execute_concurrent_with_args(self.session, stmt, params,
                                            concurrency=concurrency or self.concurrency,
                                            raise_on_first_error=False)

    @staticmethod
    def _has_vector_index(meta) -> bool:
        return any("similarity_function" in opts or str(getattr(meta.columns.get(col), "cql_type", "")).startswith("vector")
                   for col, (_, opts) in _index_details(meta).items())

    def _write_all(self, stmt, params, vector: bool) -> None:
        """Run idempotent writes; on a table with a vector index, cap the concurrency and retry
        the writes the index refused because too many were in flight, halving the concurrency
        each time down to one write at a time (which the index always accepts): the index's
        limit follows the server's CPU count, which the client doesn't know."""
        if not vector:
            for ok, res in self._concurrent(stmt, params):
                if not ok:
                    raise res
            return
        pending = list(params)
        conc = min(self.concurrency, self.vector_write_concurrency)
        attempt = 0
        while True:
            results = self._concurrent(stmt, pending, conc)
            failed = [(p, res) for p, (ok, res) in zip(pending, results) if not ok]
            if not failed:
                if attempt:
                    log.info("vector writes went through at %s in flight (vector_write_concurrency is %s: "
                             "lower it for this Cassandra)", conc, self.vector_write_concurrency)
                return
            refused = all("outstanding pooled objects" in str(r) or "Replica(s) failed" in str(r) for _, r in failed)
            if not refused or (conc == 1 and attempt >= 3):
                log.warning("%s write(s) to a vector-indexed table failed: %s", len(failed), failed[0][1])
                raise failed[0][1]
            log.debug("vector index refused %s of %s write(s) at %s in flight: retrying", len(failed), len(pending), conc)
            pending = [p for p, _ in failed]
            conc = max(1, conc // 2)
            time.sleep(0.05 * 2 ** min(attempt, 4))
            attempt += 1

    def _is_searchable(self, table: str, meta, key: str) -> bool:
        if self.searchable is not None:
            return key in self.searchable.get(table, ())
        if key in [c.name for c in meta.partition_key]:
            return True
        for idx in getattr(meta, "indexes", {}).values():
            target = (idx.index_options or {}).get("target", "")
            target = target.replace("values(", "").replace(")", "").strip('"')
            if target == key:
                return True
        return False

    def columns(self, kind, type_name):
        return list(self._meta(kind, type_name)[1].columns)

    def validate_type(self, kind, type_name):
        self._meta(kind, type_name)

    def validate_columns(self, kind, type_name, cols):
        table, meta = self._meta(kind, type_name)
        bad = [c for c in cols if c not in meta.columns or c == self.id_column]
        if bad:
            raise KeyError(f"Table {table!r} has no writable column(s) {bad}")

    def write_rows(self, kind, type_name, rows):
        """INSERT = upsert; only the given columns are written (others untouched)."""
        table, meta = self._meta(kind, type_name)
        by_cols: dict[tuple, list] = {}
        for id_, props in rows:
            cols = tuple(sorted(props))
            self.validate_columns(kind, type_name, cols)
            by_cols.setdefault(cols, []).append((id_, props))
        for cols, items in by_cols.items():
            names = [self.id_column, *cols]
            cql = (f'INSERT INTO "{self.keyspace}"."{table}" '
                   f'({", ".join(chr(34) + n + chr(34) for n in names)}) '
                   f'VALUES ({", ".join("?" for _ in names)})')
            params = [(self._id_param(meta, id_), *[p[c] for c in cols]) for id_, p in items]
            self._write_all(self._prepared(cql), params, self._has_vector_index(meta))

    def delete_rows(self, kind, type_name, ids):
        table, meta = self._meta(kind, type_name)
        stmt = self._prepared(
            f'DELETE FROM "{self.keyspace}"."{table}" WHERE "{self.id_column}" = ?')
        for ok, res in self._concurrent(stmt, [(self._id_param(meta, i),) for i in ids]):
            if not ok:
                raise res

    def semijoin_ids(self, kind, type_name, key, values, limit):
        try:
            table, meta = self._meta(kind, type_name)
        except KeyError:
            return set()
        if key not in meta.columns:
            return set()
        indexed = self._is_searchable(table, meta, key)
        if not indexed and not self.allow_filtering:
            return None
        cql = (f'SELECT "{self.id_column}" FROM "{self.keyspace}"."{table}" '
               f'WHERE "{key}" = ? LIMIT ?')
        if not indexed:
            cql += " ALLOW FILTERING"
        ids: set = set()
        for ok, res in self._concurrent(self._prepared(cql), [(v, limit + 1) for v in values]):
            if not ok:
                raise res
            for row in res:
                ids.add(str(row._asdict()[self.id_column]))
        return ids if len(ids) <= limit else None

    def scan_ids(self, kind, type_name):
        """Full id scan of one table (paged by the driver). Cost grows with table size."""
        table, _ = self._meta(kind, type_name)
        stmt = self._prepared(f'SELECT "{self.id_column}" FROM "{self.keyspace}"."{table}"')
        try:
            stmt.fetch_size = 5000
        except Exception:
            pass
        for row in self.session.execute(stmt):
            yield str(row[0])

    def scan_rows(self, kind, type_name):
        """Full table scan, paged by the driver (5000 rows a page)."""
        table, _ = self._meta(kind, type_name)
        stmt = self._prepared(f'SELECT * FROM "{self.keyspace}"."{table}"')
        try:
            stmt.fetch_size = 5000
        except Exception:
            pass
        for row in self.session.execute(stmt):
            d = self._row(row)
            yield d.pop(self.id_column), {k: v for k, v in d.items() if v is not None}

    def truncate(self, kind, type_name):
        table, _ = self._meta(kind, type_name)
        self.session.execute(f'TRUNCATE "{self.keyspace}"."{table}"', timeout=120)
        self._cache.clear()

    def table_columns(self, kind, type_name):
        _, meta = self._meta(kind, type_name)
        return set(meta.columns) - {self.id_column}

    def _id_param(self, meta, id_: str):
        """Ids are canonical UUID strings. A `text` id column takes them as-is;
        a native `uuid` column needs uuid.UUID (detected from table metadata)."""
        col = meta.columns.get(self.id_column)
        if str(getattr(col, "cql_type", "") or "").lower() == "uuid":
            return _uuid.UUID(id_)
        return id_

    def _row(self, row) -> dict:
        d = row._asdict()
        if self.id_column in d and d[self.id_column] is not None:
            d[self.id_column] = str(d[self.id_column])   # always hand back str ids
        return d

    def fetch_rows(self, kind, type_name, ids):
        table, meta = self._meta(kind, type_name)
        stmt = self._prepared(
            f'SELECT * FROM "{self.keyspace}"."{table}" WHERE "{self.id_column}" = ?')
        results = self._concurrent(stmt, [(self._id_param(meta, i),) for i in ids])
        out = {}
        for i, (ok, res) in zip(ids, results):
            if not ok:
                raise res
            row = next(iter(res), None)
            out[i] = self._row(row) if row is not None else {}
        return out

    def search(self, kind, type_name, key, values):
        table, meta = self._meta(kind, type_name)
        if key not in meta.columns:  # validates `key` before it's put in CQL
            return []
        indexed = self._is_searchable(table, meta, key)
        if not indexed and not self.allow_filtering:
            return None
        cql = f'SELECT * FROM "{self.keyspace}"."{table}" WHERE "{key}" = ?'
        if not indexed:
            cql += " ALLOW FILTERING"
        results = self._concurrent(self._prepared(cql), [(v,) for v in values])
        out = []
        for ok, res in results:
            if not ok:
                raise res
            for row in res:
                d = self._row(row)
                out.append((d[self.id_column], d))
        return out

    def vector_search(self, kind, type_name, field, vector, k, filters, similarity):
        """One approximate nearest-neighbour query (SAI vector index), filters applied inside
        Cassandra: SELECT id, similarity_<fn>(field, ?) ... WHERE c = ? ... ORDER BY field ANN OF ?"""
        table, meta = self._meta(kind, type_name)
        for c in (field, *filters):                 # validates every name before it goes into CQL
            if c not in meta.columns:
                raise OpenCypherError(f"table {table!r} has no column {c!r}")
        idx = _index_details(meta)
        if field not in idx:
            raise OpenCypherError(f"{table}.{field} has no vector index (use VectorField in the model, "
                                  f"then run the migration)")
        for c in filters:
            if c not in idx or not _is_sai(idx[c][1]):
                raise OpenCypherError(f"vector.search can only filter on SAI-indexed fields; "
                                      f"{table}.{c} has no SAI index")
        where = " AND ".join(f'"{c}" = ?' for c in filters)
        cql = (f'SELECT "{self.id_column}", similarity_{similarity}("{field}", ?) AS score '
               f'FROM "{self.keyspace}"."{table}"' + (f" WHERE {where}" if where else "")
               + f' ORDER BY "{field}" ANN OF ? LIMIT ?')
        rows = self.session.execute(self._prepared(cql), (vector, *filters.values(), vector, k))
        return [(str(r[0]), float(r[1])) for r in rows]


_REV_MARKER = "00000000-0000-0000-0000-000000000000"   # row that says "the table is complete"


class _StoreIdIndex:
    """uuid -> vertex index / Edge kept in the store's reverse table (default name
    `reverse_vertex_ids`) instead of a Python dict, so startup does not scan the graph.

    graph-tool's own id property map stays the source of truth: every hit is validated against
    it, so a stale row can never return the wrong element. A row whose element no longer exists is
    simply a miss. If a vertex row points at a DIFFERENT live id (the graph was renumbered), the
    index degrades to an in-memory dict built once from the graph (with a warning) -- run
    `cy.repair(reverse="rebuild")` to fix the table. Writes are staged with the entity rows and
    flushed/rolled back together with them."""
    external = True

    def __init__(self, sc: "Schema", kind: str, store: "PropertyStore"):
        self.sc, self.kind, self.store = sc, kind, store
        self._tag = "v" if kind == "vertex" else "e"
        self._fb: dict | None = None             # degraded in-memory index

    def prefetch(self, ids: Iterable[str]) -> None:
        if self._fb is None:
            self.store.prefetch_keys("reverse", "ids", ids)

    def _build_fallback(self) -> dict:
        sc = self.sc
        g = sc._graph
        pm = (g.vp if self.kind == "vertex" else g.ep)[sc.id_map]
        fb = {}
        for el in (g.vertices() if self.kind == "vertex" else g.edges()):
            i = pm[el]
            if i:
                fb[norm_id(i)] = int(el) if self.kind == "vertex" else el
        self._fb = fb
        return fb

    def get(self, id_: str, default: Any = None) -> Any:
        if self._fb is not None:
            r = self._fb.get(id_)
            return default if r is None else r
        row = self.store.row("reverse", "ids", id_)
        if not row or row.get("kind") != self._tag:
            return default
        sc, g = self.sc, self.sc._graph
        try:
            idx = int(row["idx"])
            if self.kind == "vertex":
                gid = g.vp[sc.id_map][g.vertex(idx)]
                if gid == id_:
                    return idx
                if not gid:                       # slot empty / tombstoned: the row is an orphan
                    return default
                sc.stale_hits += 1                # slot holds ANOTHER live id: graph was renumbered
                warnings.warn(
                    f"reverse id index disagrees with the graph (id {id_}); using an in-memory index "
                    "for this process. Run cy.repair(reverse='rebuild').", RuntimeWarning, stacklevel=3)
                r = self._build_fallback().get(id_)
                return default if r is None else r
            for e in g.edge(g.vertex(int(row["src"])), g.vertex(int(row["dst"])), all_edges=True) or ():
                if g.ep[sc.id_map][e] == id_:     # the edge itself is authoritative, not its index
                    if int(g.edge_index[e]) != idx:
                        sc.stale_hits += 1
                    return e
            return default
        except (ValueError, KeyError, TypeError, IndexError):
            return default

    def __contains__(self, id_: str) -> bool:
        return self.get(id_) is not None

    def __getitem__(self, id_: str) -> Any:
        r = self.get(id_)
        if r is None:
            raise KeyError(id_)
        return r

    def __setitem__(self, id_: str, el: Any) -> None:
        g = self.sc._graph
        if self.kind == "vertex":
            props = {"kind": "v", "idx": int(el)}
        else:
            props = {"kind": "e", "idx": int(g.edge_index[el]),
                     "src": int(el.source()), "dst": int(el.target())}
        self.store.stage_write("reverse", "ids", id_, props)
        if self._fb is not None:
            self._fb[id_] = int(el) if self.kind == "vertex" else el

    def pop(self, id_: str, default: Any = None) -> Any:
        self.store.stage_delete("reverse", "ids", id_)
        if self._fb is not None:
            self._fb.pop(id_, None)
        return default

    def clear(self) -> None:                      # never wipes the table: just drops the fallback
        self._fb = None
