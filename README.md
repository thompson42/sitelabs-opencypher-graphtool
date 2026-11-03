# sitelabs-opencypher-graphtool

**opencypher_graphtool**: an openCypher query engine for [graph-tool](https://graph-tool.skewed.de/)
graphs. The topology lives in graph-tool (in memory); node and relationship properties live in a
pluggable property store: in memory, or Apache Cassandra (one table per type). A pydantic data
model is the single source of truth for types, labels, tables and validation.

It is the engine shared by:

* **sitelabs-graph-database**: the graph database (write server, read replicas, failover,
  Cassandra), and
* **sitelabs-partitioned-graph**: an in-memory graph partitioned over many servers.

## Install

graph-tool is not on PyPI: it comes from conda-forge, so the engine is installed into a conda
environment that already has it.

```bash
conda env create -f environment.yml          # python, graph-tool, then this package (editable)
conda activate opencypher-graphtool
python -m pytest                             # the tests (Cassandra ones skip without it)
```

In another project, with graph-tool in its environment:

```bash
pip install -e ../sitelabs-opencypher-graphtool[cassandra]        # while developing both side by side
pip install "opencypher-graphtool[cassandra] @ git+https://github.com/thompson42/sitelabs-opencypher-graphtool.git@v0.1.0"
```

The `cassandra` extra adds the Cassandra driver (for `CassandraStore`); without it the engine runs
on `InMemoryStore` or a store of your own.

## Use

```python
from graph_tool import Graph
from opencypher_graphtool import EdgeModel, Field, InMemoryStore, Models, VertexModel, opencypher


class Person(VertexModel):
    class Meta:
        type = "Person"
    name: str = Field(index=True)


class Knows(EdgeModel):
    class Meta:
        type = "KNOWS"
        source, target = "Person", "Person"     # descriptive
    since: int | None = None


cy = opencypher(Graph(), InMemoryStore({}), models=Models([Person, Knows]))
cy.run("CREATE (:Person {name: 'Ann'})-[:KNOWS {since: 2020}]->(:Person {name: 'Bob'})")
print(cy.run("MATCH (a:Person)-[k:KNOWS]->(b) RETURN a.name AS a, b.name AS b, k.since AS s").data())
```

With Cassandra: `CassandraStore(session, "keyspace")` instead of `InMemoryStore({})`. More in
[`examples/`](examples).

## The modules

The engine was one 6,400-line module (`opencypher_gt`); it is now a package of modules, in the
order they load:

| Module | What |
|---|---|
| `errors` | The exceptions (`OpenCypherError`, `ConstraintError`, lock errors, ...) |
| `locking` | One fair readers-writer lock per graph (`GraphLock`, `lock_of`) |
| `common` | The write transaction record, sentinels, core keys |
| `ids` | Element ids: UUIDs, natural-key ids (`bulk_node_id`, `stable_id`) |
| `lexer`, `ast_nodes`, `parser` | openCypher text to a syntax tree (`parse`) |
| `values` | Values, null logic, ordering, the built-in functions |
| `results` | `Node`, `Rel`, `Path`, `Result` |
| `models` | The data model: `Models`, `VertexModel`, `EdgeModel`, `Field`, `Vector`, migrations |
| `schema` | Labels and types as bitmasks, UUID ids, attached to a graph-tool `Graph` |
| `elements` | Creating, changing and deleting vertices and edges, with undo for transactions |
| `stores` | The property store interface (`PropertyStore`) and `InMemoryStore` |
| `cassandra_store` | `CassandraStore` (the `cassandra` extra) |
| `execution`, `graph_functions`, `matching`, `clauses` | The query runner, built up module by module: core, graph functions, `MATCH` (pattern expansion), the clauses |
| `procedures` | `CALL` procedures (PageRank, components, shortest paths, `vector.search`, ...) |
| `engine` | `OpenCypher`, `opencypher()`, `VerifyReport` |

**How the modules import each other.** Each module imports what it needs from the modules before it
at the top. The few names it needs from modules after it are bound by its `_link()` function, which
the package's `__init__` calls once every module is loaded. Import the public API from the package
(`from opencypher_graphtool import ...`); importing a module directly works too, since the package
loads first.

## Extension points

* **Property stores:** subclass `PropertyStore` (see `InMemoryStore` and `CassandraStore`): the
  columns of a type, fetching rows by id, writing them, and optional index search.
* **Pattern expansion:** `matching` expands one hop of a pattern on the local graph-tool graph. The
  partitioned graph needs this step to become an exchange with the partitions that own the
  vertices; making it a documented hook is the next change here (planned with
  sitelabs-partitioned-graph).

## Tests

```bash
python -m pytest                              # everything
REQUIRE_CASSANDRA=1 python -m pytest          # fail instead of skipping without Cassandra (CI)
```

The Cassandra tests (`tests/integration/`) need Cassandra on `127.0.0.1:9042`, e.g.
`docker run -d -p 127.0.0.1:9042:9042 cassandra:5.0.9`.

## Versions

Releases are git tags (`v0.1.0`, ...). The projects using the engine pin a tag in their
requirements, and move to a new one deliberately.

## Licence

Apache License 2.0 ([LICENSE.txt](LICENSE.txt)); third-party names in [NOTICES.txt](NOTICES.txt).
