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

"""opencypher_graphtool: an openCypher query engine for graph-tool, with optional external property
storage (e.g. Apache Cassandra). It carries its own storage layer.

The package's modules: errors, locking, common, ids, lexer, ast_nodes, parser, values, results,
models, schema, elements, stores, cassandra_store, execution, graph_functions, matching, clauses,
procedures, engine (see README.md). The public API is imported here: `from opencypher_graphtool
import opencypher, Schema, Models, ...`.

    from graph_tool import Graph
    from opencypher_graphtool import opencypher, Schema, CassandraStore

    cy = opencypher(Graph(), CassandraStore(session, "ks"), Schema(...))
    cy.run("CREATE (:Person {name: $n, age: $a})", n="Ann", a=30)
    cy.run("MATCH (p:Person)-[:KNOWS*1..3]->(q) WHERE p.name = 'Ann' RETURN DISTINCT q.name").data()

Storage model
-------------
* graph-tool keeps topology + three core properties per element:
    id    -> UUID string (canonical lowercase)  -- id(n), n.id
    label -> int32/int64 BITMASK, one bit per label (a node may have several labels)
    type  -> one bit: selects the store table (default: the node's FIRST label; for a
             relationship, its type name)
* EVERY other property lives in the store, one table per node type and per relationship
  type, keyed by id. `id`, `label` and `type` are therefore reserved property names.
* A node has ANY NUMBER of labels (bitmask) but exactly ONE type (its table). A relationship
  has exactly ONE type (:KNOWS) and NO labels: edges carry only the `type` bit map.
  Names are auto-registered on first use (Schema(auto_register=True), 32/64 per registry).

Supported openCypher
--------------------
Clauses   MATCH, OPTIONAL MATCH, WHERE, WITH, RETURN [DISTINCT], ORDER BY, SKIP, LIMIT,
          UNWIND, CREATE, MERGE (ON CREATE / ON MATCH SET), SET (n.p = x, n += {..},
          n = {..}, n:Label), REMOVE, DELETE / DETACH DELETE, CALL ... YIELD, UNION [ALL],
          EXPLAIN.
Patterns  labels (:A:B), inline properties, relationship types (:A|B), directions,
          variable length (*, *2, *1..3, *..4), path variables, shortestPath(),
          allShortestPaths(), multiple comma-separated parts, relationship uniqueness
          per MATCH (as in openCypher), pattern predicates in WHERE, EXISTS { pattern },
          pattern comprehensions.
Exprs     arithmetic, comparison, three-valued logic, IN, STARTS/ENDS WITH, CONTAINS,
          =~, IS [NOT] NULL, CASE, list/map literals, indexing and slicing, list
          comprehensions, any/all/none/single, reduce, parameters ($name),
          aggregates (count sum avg min max collect stDev stDevP percentileCont/Disc).
Functions id labels type keys properties nodes relationships length startNode endNode
          exists size head last tail range reverse coalesce toInteger toFloat toString
          toBoolean abs ceil floor round sign sqrt exp log log10 trig rand toUpper
          toLower trim replace substring left right split timestamp randomUUID degree.
Procedures db.labels, db.relationshipTypes, db.propertyKeys and graph-tool backed
          gt.pageRank, gt.connectedComponents, gt.peerPressure, gt.shortestPath.

Performance
-----------
* Node scans with labels run as numpy bit tests; label-less scans are a plain loop.
* A start node with an inline property (or WHERE x.p = v / x.p IN [..] / x:Label) is found
  with one store search when the column is searchable; the planner starts from the
  most selective end of the pattern (bound variable > equality property > rarest label).
* After each hop, property predicates are checked in batches: store rows are prefetched
  per chunk, and large chunks (store.semijoin_min) use ONE id-only indexed query
  (semi-join) instead of one row fetch per node.
* Execution is chunked and lazy: LIMIT without ORDER BY/aggregation stops early; writes
  are eager barriers (all reads finish first, as in openCypher) and are staged, then flushed
  to the store in batches when the query ends.
* cy.explain(query) (or `EXPLAIN ...`) shows start nodes and seed strategy.

Differences from openCypher (read these)
----------------------------------------
* One query = one transaction (atomic, see "Consistency" below); there is no BEGIN/COMMIT
  across queries. Deleted vertices are tombstoned, not physically removed, because graph-tool
  renumbers on removal.
* Unbounded variable length (*) is capped at cy.varlen_cap (64). Trail semantics
  (relationships unique per path), which can be exponential on dense graphs.
* Only equality/IN/label conditions are pushed to the store; everything else is
  evaluated in Python after rows are fetched.
* Not supported: subqueries (CALL { }), FOREACH, LOAD CSV, temporal/spatial types,
  map projections n{.x}, list-valued store columns in indexes, user-defined procedures
  (register your own in _PROCS), schema commands (indexes/constraints).

Concurrency (graph-tool property maps are not thread-safe): one fair readers-writer lock per
graph, shared by everything that drives the same Graph.
* `cy.run()` of a query with CREATE/MERGE/SET/REMOVE/DELETE takes the lock exclusively
  (the staged writes are flushed to the store before it is released); read queries share it.
  `cy.is_write(query)` tells which. Waiters queue first-come-first-served (threads and asyncio
  tasks share one queue), so concurrent MERGEs of the same key are safe.
* `await cy.run_async(query, **params)` queues without blocking the event loop.
* `with cy.lock.write():` groups several statements atomically (re-entrant for the holder);
  read->write upgrade raises LockUpgradeError; `lock.write(timeout=..)` raises LockTimeout.
* Per-process only. `opencypher(..., lock=False)` disables it (graph-wide flag).

Consistency between graph-tool and the store (all on by default):
* Atomic writes. A write query is all-or-nothing. graph-tool changes are journaled and undone
  newest-first if anything fails (query error, store down, ...), vertices created by the failed
  query are really removed (no tombstones), and the store is compensated: rows it had already
  written are deleted / restored from their pre-images (read just before the first flush).
  Other threads never see the uncommitted state (the exclusive lock is held until the end).
  Failures before anything was written (e.g. the store is down) leave nothing behind at all.
* If the rollback itself cannot finish (store died mid-flush and stays down) a ConsistencyError is
  raised; the graph is already rolled back and the pending corrections are kept in
  `store.unresolved`. They are retried automatically at the start of the next write query, or by
  `cy.repair()`. They live in memory: a process crash loses them -- use verify() after a crash.
* `cy.verify()` compares ids graph <-> every store table (missing rows / orphan rows / missing
  tables / unresolved corrections). `cy.repair(missing=, orphans=)` fixes drift per policy:
  missing "report" | "recreate" (empty row) | "tombstone"; orphans "report" | "delete".
  Both need `store.scan_ids()` (implemented by InMemoryStore and CassandraStore; one full id
  scan per table). `opencypher(..., atomic=False)` restores the old flush-at-end behaviour.
* MERGE creates elements with deterministic ids (uuid5 of label(s)/type + properties [+ endpoint
  ids]), so retrying a MERGE after a crash or a lost graph file re-creates the SAME id and the
  store upsert is idempotent. (Falls back to a random id if that id is already taken, e.g. the
  node was renamed since.) `opencypher(..., stable_merge_ids=False)` disables it.
* Not covered: persisting the graph-tool file itself, and crashes between the Cassandra write and
  your graph save -- verify()/repair() are the recovery path. Newly auto-registered labels/types
  are not un-registered by a rollback (harmless: an unused bit).

Fast startup: the reverse id table (`opencypher(..., id_index="store")`).
* By default the id -> vertex/edge index is a Python dict built at startup by one pass over the
  whole graph (O(V+E)). With id_index="store" it lives in a Cassandra table instead
  (default name `reverse_vertex_ids`, one row per vertex AND edge: id, kind 'v'/'e', idx, and
  src/dst for edges) and startup reads one row. Create it once:
      store.create_reverse_table("uuid")   # or "text"; CREATE TABLE ... (id uuid PRIMARY KEY,
                                           #   kind text, idx bigint, src bigint, dst bigint)
  The first start on an existing graph builds the table (one-time O(V+E), with a warning); a
  `00000000-0000-0000-0000-000000000000` marker row says "complete". `cy.rebuild_id_index()` redoes it.
* Rows are written/deleted with the entity rows in the same flush, so they are part of the atomic
  write / rollback machinery above. Lookups are batched (one round trip per search result set)
  and cached. Cost: one extra row write per created element and one extra read per uncached id.
* graph-tool stays the source of truth: every hit is checked against the graph's own id map. A
  row for a vanished element is a clean miss; a vertex row whose slot holds a DIFFERENT live id
  (graph renumbered) degrades this process to an in-memory index with a warning; an edge is
  found by its endpoints and id, so a drifted edge index self-heals. A MISSING row is treated as
  "no such element" -- after restoring a graph file or the table from a backup run
  `cy.verify()` (reports reverse_missing / reverse_stale / reverse_orphans) and
  `cy.repair(reverse="rebuild")`.

Durability and limits (hooks for the host application):
* `cy.on_commit = fn`: fn(ops) is called once per committed write query, after the store flush and
  while the write lock is held, with the query's graph-tool topology changes as JSON-able, id-keyed
  ops. Write them to a durable journal; raising rolls the query back. `cy.apply_redo(ops)` replays
  them onto a graph (graph-tool only -- the store already has the rows), idempotently. Queries are
  never re-executed, so read-modify-write SETs are not applied twice.
* `cy.run_once(request_id, query, params)`: an idempotent write. The id is committed with the
  write (an `["rq", id, stats]` op in the same on_commit call, so in the same journal entry);
  a second call with the same id runs nothing and returns the first one's stats. apply_redo()
  learns the ids too, so a follower promoted to leader recognises a retry. The newest
  `cy.request_id_retain` ids are kept (`cy.request_ids`, saved with snapshots by the host).
* `cy.bulk_nodes(...)` / `cy.bulk_relationships(...)`: file imports without openCypher. Node ids
  come from the type and a natural key (`bulk_node_id`), so relationships find their endpoints
  without a search, and re-loading a row updates it. Same transaction, journal, validation and
  rollback as a write query; `cy.bulk_once(request_id, fn)` makes one idempotent.
* `cy.timeout = seconds`: hot loops check a deadline and raise QueryTimeout (writes roll back).
* `cy.close()` drops the graph's registration so a replaced graph can be garbage collected.
* CassandraStore(read_consistency=, write_consistency=, concurrency=) tune the driver calls.

Pydantic models = the single source of truth (`pip install pydantic`; optional otherwise):
      class Person(VertexModel):                  # one model = one vertex type = one table
          class Meta: type = "Person"             # (or class Person(VertexModel, table="people"))
          name: str = Field(index=True)           # index=True -> Cassandra secondary index
          age: int | None = Field(default=None, ge=0)
          status: Status = Status.ACTIVE          # defaults are WRITTEN when a node is created
      class Knows(EdgeModel):
          since: int = Field(ge=1900)             # required on every KNOWS edge
      cy = opencypher(graph, store, models=Models([Person, Knows]))
* Schema: types/labels (and their bits) come from the models; `Meta.labels` adds extra labels;
  `(:Admin:Person)` is stored in the first label that has a model. The name->bit map is saved in the
  graph automatically and only ever EXTENDED, so reordering/adding models never moves a bit.
* Tables: created and altered automatically at startup (`migrate=False` to skip): CREATE TABLE,
  `ALTER TABLE ADD` for new fields, CREATE INDEX for index=True. Python types map to CQL (str text,
  int bigint, float double, bool boolean, datetime timestamp, UUID uuid, Enum text, list/set/dict
  collections; `Field(cql="...")` overrides). Never automatic: a changed column type or a dropped
  field (the column is left alone with a warning) -> `SchemaMigrationError`, nothing changed.
  `models.ddl(ks)`, `models.plan(store)` and `models.apply(store)` expose the same steps.
* Validation: a value is type-checked/converted when SET (OpenCypherError at once); at COMMIT every row
  the query touched is validated as a WHOLE (required fields, @field_validator/@model_validator,
  defaults) and a violation raises ConstraintError and rolls the query back. Needs atomic=True;
  properties not in the model are rejected (extra="forbid").
"""
# The modules, in dependency order: each imports what it needs from the ones before it...
from . import errors  # noqa: F401
from . import locking  # noqa: F401
from . import common  # noqa: F401
from . import ids  # noqa: F401
from . import lexer  # noqa: F401
from . import ast_nodes  # noqa: F401
from . import parser  # noqa: F401
from . import values  # noqa: F401
from . import results  # noqa: F401
from . import models  # noqa: F401
from . import schema  # noqa: F401
from . import elements  # noqa: F401
from . import stores  # noqa: F401
from . import cassandra_store  # noqa: F401
from . import execution  # noqa: F401
from . import graph_functions  # noqa: F401
from . import matching  # noqa: F401
from . import clauses  # noqa: F401
from . import procedures  # noqa: F401
from . import engine  # noqa: F401

# ... then the names each one uses from the ones after it.
parser._link()
models._link()
schema._link()
elements._link()
stores._link()
execution._link()
clauses._link()

from .errors import OpenCypherError, OpenCypherSyntaxError, LockError, LockUpgradeError, LockTimeout, ConsistencyError, QueryTimeout, ConstraintError, SchemaMigrationError
from .locking import GraphLock, lock_of
from .ids import new_id, norm_id, stable_id
from .parser import parse
from .results import Result, Node, Rel, Path
from .models import Models, VertexModel, EdgeModel, Field, MigrationPlan, Vector, VectorField
from .schema import Schema, Registry
from .stores import PropertyStore, InMemoryStore
from .cassandra_store import CassandraStore, REVERSE_ID_TABLE
from .graph_functions import VECTOR_SIMILARITIES
from .engine import opencypher, OpenCypher, VerifyReport

__all__ = ['opencypher', 'OpenCypher', 'Result', 'Node', 'Rel', 'Path', 'OpenCypherError', 'OpenCypherSyntaxError', 'PropertyStore', 'InMemoryStore', 'CassandraStore', 'Schema', 'Registry', 'new_id', 'norm_id', 'stable_id', 'parse', 'GraphLock', 'LockError', 'LockUpgradeError', 'LockTimeout', 'lock_of', 'ConsistencyError', 'QueryTimeout', 'VerifyReport', 'REVERSE_ID_TABLE', 'Models', 'VertexModel', 'EdgeModel', 'Field', 'MigrationPlan', 'ConstraintError', 'SchemaMigrationError', 'Vector', 'VectorField', 'VECTOR_SIMILARITIES']
