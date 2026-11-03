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

"""opencypher_gt usage demo: queries, parameters, atomic blocks, threads, asyncio."""
import asyncio, threading
from graph_tool import Graph
from opencypher_graphtool import opencypher, Schema, InMemoryStore, LockUpgradeError, LockTimeout

# --- setup: label/type bit maps, one table per vertex/edge type, a property store -------
schema = Schema(vertex_labels=["Person", "Movie"], vertex_types=["Person", "Movie"], edge_types=["KNOWS", "ACTED_IN"])
tables = {("vertex", "Person"): {}, ("vertex", "Movie"): {},
          ("edge", "KNOWS"): {}, ("edge", "ACTED_IN"): {}}
store = InMemoryStore(tables, indexed={("vertex", "Person"): {"name"}, ("vertex", "Movie"): {"title"},
                                       ("edge", "KNOWS"): set(), ("edge", "ACTED_IN"): set()})
cy = opencypher(Graph(directed=True), store, schema)     # for Cassandra: opencypher(Graph(), CassandraStore(session, "ks"), schema)

# --- 1. writes and reads (locking is automatic) ------------------------------------------
cy.run("CREATE (:Person {name: $n, age: $a})", n="Ann", a=30)       # exclusive write lock
cy.run("CREATE (:Person {name: 'Bob', age: 25}), (:Movie {title: 'Heat'})")
cy.run("""MATCH (a:Person {name:'Ann'}), (b:Person {name:'Bob'}), (m:Movie {title:'Heat'})
          CREATE (a)-[:KNOWS {since: 2020}]->(b), (b)-[:ACTED_IN {role:'Neil'}]->(m)""")

print(cy.run("MATCH (p:Person) WHERE p.age > $min RETURN p.name AS name, p.age ORDER BY p.age",
             min=20).data())                                         # shared read lock
print(cy.run("MATCH (a:Person)-[:KNOWS*1..3]->(x)-[r:ACTED_IN]->(m:Movie) "
             "RETURN a.name, x.name, r.role, m.title").data())
print(cy.run("MATCH (p:Person) RETURN count(*) AS c, avg(p.age) AS mean").single())
print(cy.explain("MATCH (p:Person {name:'Ann'})-[:KNOWS]->(q) RETURN q.name"))   # plan, takes no lock

# --- 2. upserts and updates ----------------------------------------------------------------
cy.run("MERGE (p:Person {name:'Cy'}) ON CREATE SET p.age = 40 ON MATCH SET p.seen = true")
cy.run("MATCH (p:Person {name:'Bob'}) SET p.age = p.age + 1")
print(cy.is_write("MATCH (n) SET n.x = 1"), cy.is_write("MATCH (n) RETURN n"))   # True False

# --- 3. threads: concurrent MERGEs queue, so exactly one 'Dee' is created ----------------
ts = [threading.Thread(target=lambda: cy.run("MERGE (:Person {name:'Dee'})")) for _ in range(8)]
[t.start() for t in ts]; [t.join() for t in ts]
print("Dee nodes:", cy.run("MATCH (p:Person {name:'Dee'}) RETURN count(*)").value())

# --- 4. several statements as one atomic unit (read-modify-write) ---------------------------
with cy.lock.write():
    n = cy.run("MATCH (p:Person) RETURN count(*)").value()
    cy.run("CREATE (:Person {name: $n})", n=f"person{n}")        # nested calls are re-entrant

# --- 5. asyncio: callers queue without blocking the event loop ------------------------------
async def main():
    await asyncio.gather(*[cy.run_async("CREATE (:Person {name: $n})", n=f"async{i}") for i in range(5)])
    r = await cy.run_async("MATCH (p:Person) RETURN count(*) AS c")
    print("people:", r.value())
asyncio.run(main())

# --- 6. algorithms and misuse ----------------------------------------------------------------
print(cy.run("CALL gt.pageRank() YIELD node, score RETURN node.name, score ORDER BY score DESC LIMIT 3").data())
with cy.lock.read():
    try:
        cy.run("CREATE (:Person {name:'x'})")                    # write while holding only a read lock
    except LockUpgradeError as ex:
        print("refused:", str(ex)[:45], "...")
print(cy.lock.stats)                                             # reads / writes / waits / max_queue
# cy.lock.write(timeout=2) raises LockTimeout if the queue is too long

# --- 7. consistency: atomic writes, verify(), repair() --------------------------------------
class DownStore(InMemoryStore):                 # stand-in for "Cassandra is down"
    down = False
    def write_rows(self, *a):
        if self.down: raise IOError("cassandra down")
        return super().write_rows(*a)
store.__class__ = DownStore
store.down = True
try:
    cy.run("CREATE (:Person {name: 'Ghost'})")  # graph-tool change is rolled back, error is raised
except IOError as ex:
    print("write rejected:", ex)
store.down = False
print(cy.run("MATCH (p:Person {name:'Ghost'}) RETURN count(*)").value())     # 0: nothing half-applied

print(cy.verify())                              # compares ids in graph-tool with every table
stray = "11111111-1111-4111-8111-111111111111"
tables[("vertex", "Movie")][stray] = {"id": stray}                           # simulate drift
print(cy.verify().orphan_rows)
fix = cy.repair(missing="recreate", orphans="delete")   # policies: report | recreate | tombstone / delete
print(fix["orphan_rows_deleted"], fix["after"].ok)

# MERGE ids are deterministic, so retrying a MERGE after a crash re-creates the same ids
cy.run("MERGE (:Person {name: 'Zoe'})")

# --- 8. fast startup: keep the id -> index map in a Cassandra table instead of building it ---
#   store = CassandraStore(session, "ks", table_namer=...)
#   store.create_reverse_table("uuid")                  # once; table name: reverse_vertex_ids
#   cy = opencypher(graph, store, schema, id_index="store") # startup reads ONE row, no graph scan
#   cy.verify()                                         # also checks the reverse table
#   cy.repair(reverse="rebuild")                        # rewrite it from graph-tool if it drifted
# (graph-tool remains the source of truth: every lookup is validated against its own id map)
demo_graph = Graph(directed=True)
demo_store = InMemoryStore({("vertex", "Person"): {}, ("vertex", "Movie"): {},
                            ("edge", "KNOWS"): {}, ("edge", "ACTED_IN"): {}})
fast_schema = Schema(vertex_labels=["Person", "Movie"], vertex_types=["Person", "Movie"], edge_types=["KNOWS", "ACTED_IN"])   # one Schema per graph
fast = opencypher(demo_graph, demo_store, fast_schema, id_index="store")
fast.run("CREATE (:Person {name:'Ann'})-[:KNOWS]->(:Person {name:'Bob'})")
print(len(demo_store.tables[("reverse", "ids")]), "reverse rows (2 vertices + 1 edge + marker)")

# --- 9. pydantic models as the single source of truth ----------------------------------------
from opencypher_graphtool import Models, VertexModel, EdgeModel, Field, ConstraintError

class Person2(VertexModel):                       # one model = one type = one table
    class Meta:
        type = "Person"
    name: str = Field(index=True)                 # -> Cassandra secondary index
    age: int | None = Field(default=None, ge=0)
    tags: list[str] = Field(default_factory=list)

class Knows2(EdgeModel):
    class Meta:
        type = "KNOWS"
    since: int = Field(ge=1900)                   # required on every KNOWS edge

models = Models([Person2, Knows2])
print(models.ddl("mykeyspace")[:2])               # the CREATE TABLE / CREATE INDEX it will run
# Cassandra: store = CassandraStore(session, "mykeyspace"); the tables are created/altered at startup
mcy = opencypher(Graph(directed=True), InMemoryStore({}), models=models)
mcy.run("CREATE (a:Person {name:'Ann', age:'30'})-[:KNOWS {since: 2010}]->(b:Person {name:'Bob'})")
print(mcy.run("MATCH (p:Person {name:'Ann'}) RETURN p.age, p.tags").single())   # '30' became 30, tags defaulted
try:
    mcy.run("MATCH (a:Person {name:'Ann'}), (b:Person {name:'Bob'}) CREATE (a)-[:KNOWS]->(b)")   # no `since`
except ConstraintError as ex:
    print("rolled back:", ex)
