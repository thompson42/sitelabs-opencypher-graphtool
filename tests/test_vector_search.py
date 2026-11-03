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

"""Vector search (docs/vector-search.md): the Vector type, SAI index DDL and migrations,
the vector.* scoring functions and CALL vector.search (in memory)."""
from types import SimpleNamespace as NS

import pytest
from graph_tool import Graph

import opencypher_graphtool as C
from opencypher_graphtool import EdgeModel, Field, InMemoryStore, Models, OpenCypherError, Vector, VectorField, VertexModel, opencypher


class Doc(VertexModel):
    class Meta:
        type = "Doc"
    title: str = Field(index=True)
    kind: str = Field(index=True)
    tags: list[str] = Field(default_factory=list, index=True)
    embedding: Vector[3] | None = VectorField(similarity="cosine")
    unindexed: Vector[2] = Field(default_factory=lambda: [0.0, 0.0])


class Cites(EdgeModel):
    class Meta:
        type = "CITES"


def models(**kw):
    return Models([Doc, Cites], **kw)


def engine(rows=()):
    cy = opencypher(Graph(directed=True), InMemoryStore({}), models=models())
    if rows:
        cy.run("UNWIND $d AS d CREATE (:Doc {title: d.t, kind: d.k, embedding: d.e})", d=list(rows))
    return cy


DOCS = [{"t": "a", "k": "paper", "e": [1, 0, 0]}, {"t": "b", "k": "note", "e": [0.9, 0.1, 0]},
        {"t": "c", "k": "paper", "e": [0, 1, 0]}, {"t": "d", "k": "paper", "e": [0.7, 0.7, 0]}]


# --------------------------------------------------------------------------------------- #
# the data model
# --------------------------------------------------------------------------------------- #
def test_vector_fields_map_to_cassandra_vectors_and_validate_their_length():
    info = models()._by[("vertex", "Doc")].fields
    assert (info["embedding"].cql, info["embedding"].dim, info["embedding"].index, info["embedding"].similarity) \
        == ("vector<float, 3>", 3, True, "cosine")
    assert (info["unindexed"].cql, info["unindexed"].index, info["unindexed"].similarity) == ("vector<float, 2>", False, None)
    assert not info["embedding"].required                                # VectorField defaults to None
    m = models()
    assert m.coerce_props("vertex", "Doc", {"embedding": [1, 2, 3]}) == {"embedding": [1.0, 2.0, 3.0]}
    with pytest.raises(ValueError, match="at least 3"):
        m.coerce_props("vertex", "Doc", {"embedding": [1, 2]})
    with pytest.raises(ValueError, match="at most 3"):
        m.coerce_props("vertex", "Doc", {"embedding": [1, 2, 3, 4]})
    with pytest.raises(ValueError):
        m.coerce_props("vertex", "Doc", {"embedding": ["x", 2, 3]})


def test_vector_field_options_are_checked():
    with pytest.raises(ValueError, match="similarity must be one of"):
        VectorField(similarity="manhattan")
    with pytest.raises(TypeError, match="positive integer"):
        Vector[0]

    class NotAVector(VertexModel):
        class Meta:
            type = "NotAVector"
        x: str | None = VectorField()
    with pytest.raises(TypeError, match="needs a Vector"):
        Models([NotAVector])


def test_every_index_is_sai_and_vector_indexes_carry_their_similarity():
    ddl = models().ddl("ks")
    assert """CREATE INDEX IF NOT EXISTS "doc_title_idx" ON "ks"."doc" ("title") USING 'sai'""" in ddl
    assert """CREATE INDEX IF NOT EXISTS "doc_tags_idx" ON "ks"."doc" (VALUES("tags")) USING 'sai'""" in ddl
    assert ("""CREATE INDEX IF NOT EXISTS "doc_embedding_idx" ON "ks"."doc" ("embedding") USING 'sai' """
            """WITH OPTIONS = {'similarity_function': 'COSINE'}""") in ddl
    assert not any("unindexed" in s for s in ddl if s.startswith("CREATE INDEX"))
    assert 'CREATE TABLE IF NOT EXISTS "ks"."doc" ("id" uuid PRIMARY KEY, "title" text, "kind" text, ' \
           '"tags" list<text>, "embedding" vector<float, 3>, "unindexed" vector<float, 2>)' in ddl


def fake_store(columns: dict, indexes: dict):
    """Just enough of a CassandraStore for Models.plan(): one existing `doc` table."""
    cols = {c: NS(cql_type=t, name=c) for c, t in columns.items()}
    idx = {name: NS(index_options=opts) for name, opts in indexes.items()}
    tables = {"doc": NS(columns=cols, indexes=idx)}
    return NS(keyspace="ks", id_column="id", id_type="uuid", reverse_table="reverse_vertex_ids",
              session=NS(cluster=NS(metadata=NS(keyspaces={"ks": NS(tables=tables)}))))


DOC_COLS = {"id": "uuid", "title": "text", "kind": "text", "tags": "list<text>",
            "embedding": "vector<float, 3>", "unindexed": "vector<float, 2>"}
SAI_INDEXES = {"doc_title_idx": {"target": "title", "class_name": "sai"},
               "doc_kind_idx": {"target": "kind", "class_name": "sai"},
               "doc_tags_idx": {"target": "values(tags)", "class_name": "sai"},
               "doc_embedding_idx": {"target": "embedding", "class_name": "sai", "similarity_function": "COSINE"}}


def test_migrations_add_vector_fields_and_report_index_problems():
    m = Models([Doc])
    p = m.plan(fake_store(DOC_COLS, SAI_INDEXES))
    assert (p.statements, p.blocked, p.warnings) == ([], [], [])         # everything in place

    cols = {k: v for k, v in DOC_COLS.items() if k != "embedding"}         # a new vector field
    idx = {k: v for k, v in SAI_INDEXES.items() if k != "doc_embedding_idx"}
    p = m.plan(fake_store(cols, idx))
    assert p.statements == ['ALTER TABLE "ks"."doc" ADD "embedding" vector<float, 3>',
                            """CREATE INDEX IF NOT EXISTS "doc_embedding_idx" ON "ks"."doc" ("embedding") USING 'sai' """
                            """WITH OPTIONS = {'similarity_function': 'COSINE'}"""]

    other = dict(SAI_INDEXES, doc_embedding_idx={"target": "embedding", "class_name": "sai",
                                                 "similarity_function": "EUCLIDEAN"})
    p = m.plan(fake_store(DOC_COLS, other))                                # similarity changed
    assert len(p.blocked) == 1 and "uses EUCLIDEAN, the model says COSINE" in p.blocked[0]

    p = m.plan(fake_store(dict(DOC_COLS, embedding="vector<float, 8>"), SAI_INDEXES))   # dimension changed
    assert len(p.blocked) == 1 and "vector<float, 8>" in p.blocked[0]

    legacy = dict(SAI_INDEXES, doc_title_idx={"target": "title"})          # an older secondary index
    p = m.plan(fake_store(DOC_COLS, legacy))
    assert p.statements == [] and p.blocked == []
    assert len(p.warnings) == 1 and "older secondary index, not SAI" in p.warnings[0]


# --------------------------------------------------------------------------------------- #
# the vector.* functions
# --------------------------------------------------------------------------------------- #
def test_scores_follow_cassandras_0_to_1_scaling():
    s = C.graph_functions._vector_score
    assert s("cosine", [1, 0], [1, 0]) == 1.0 and s("cosine", [1, 0], [-1, 0]) == 0.0
    assert s("cosine", [1, 0], [0, 1]) == 0.5 and s("cosine", [2, 0], [1, 0]) == 1.0      # length-free
    assert s("dot_product", [1, 0], [0.6, 0.8]) == pytest.approx(0.8)                   # (1 + 0.6) / 2
    assert s("euclidean", [1, 0], [0, 0]) == 0.5 and s("euclidean", [1, 1], [1, 1]) == 1.0   # 1 / (1 + d²)
    assert s("cosine", [0, 0], [1, 0]) is None and s("cosine", None, [1]) is None
    with pytest.raises(OpenCypherError, match="dimensions differ"):
        s("cosine", [1, 0], [1, 0, 0])


def test_vector_functions_in_queries():
    cy = engine(DOCS)
    rows = cy.run("MATCH (d:Doc) RETURN d.title AS t, vector.cosine(d.embedding, [1, 0, 0]) AS c, "
                  "vector.euclidean(d.embedding, [1, 0, 0]) AS e, vector.dot_product(d.embedding, [1, 0, 0]) AS p "
                  "ORDER BY c DESC").data()
    assert [r["t"] for r in rows] == ["a", "b", "d", "c"]
    assert rows[1]["e"] == pytest.approx(1 / 1.02) and rows[1]["p"] == pytest.approx(0.95)
    assert cy.run("RETURN vector.cosine(null, [1]) AS n").value() is None
    assert cy.run("MATCH (d:Doc) WITH d.title AS vector RETURN count(vector) AS n").value() == 4   # still a name
    with pytest.raises(OpenCypherError, match="Unknown function 'vector.nope'"):
        cy.run("RETURN vector.nope([1], [1])")
    with pytest.raises(OpenCypherError, match="dimensions differ"):
        cy.run("MATCH (d:Doc) RETURN vector.cosine(d.embedding, [1, 0])")


def test_the_scan_guard_points_to_vector_search():
    cy = engine(DOCS)
    cy.vector_scan_limit = 3
    with pytest.raises(OpenCypherError, match="use CALL vector.search"):
        cy.run("MATCH (d:Doc) RETURN vector.cosine(d.embedding, [1, 0, 0]) AS c").data()
    cy.vector_scan_limit = None                                            # no limit
    assert len(cy.run("MATCH (d:Doc) RETURN vector.cosine(d.embedding, [1, 0, 0]) AS c").data()) == 4


# --------------------------------------------------------------------------------------- #
# CALL vector.search
# --------------------------------------------------------------------------------------- #
def search(cy, q="CALL vector.search('Doc', 'embedding', $q, $k) YIELD node, score "
                 "RETURN node.title AS t, score", **params):
    return [(r["t"], round(r["score"], 4)) for r in cy.run(q, **{"q": [1, 0, 0], "k": 3, **params}).data()]


def test_vector_search_finds_the_nearest_nodes_best_first():
    cy = engine(DOCS)
    assert search(cy) == [("a", 1.0), ("b", 0.9969), ("d", 0.8536)]
    assert search(cy, k=1) == [("a", 1.0)]
    assert search(cy, k=1000)[-1] == ("c", 0.5)                            # all four
    filtered = "CALL vector.search('Doc', 'embedding', $q, 3, {kind: 'paper'}) YIELD node, score RETURN node.title AS t, score"
    assert search(cy, filtered) == [("a", 1.0), ("d", 0.8536), ("c", 0.5)]
    assert cy.run("CALL vector.search('Doc', 'embedding', null, 3) YIELD node RETURN node").data() == []
    assert cy.run("CALL vector.search('Doc', 'embedding', [1,0,0], 3, {kind: null}) YIELD node RETURN node").data() == []
    assert cy.run("CALL vector.search('Doc', 'embedding', [1,0,0], 3) YIELD node RETURN node").summary["vector_search"] == 1


def test_vector_search_composes_with_the_graph():
    cy = engine(DOCS)
    cy.run("MATCH (a:Doc {title: 'a'}), (c:Doc {title: 'c'}), (d:Doc {title: 'd'}) "
           "CREATE (a)-[:CITES]->(c), (a)-[:CITES]->(d)")
    rows = cy.run("CALL vector.search('Doc', 'embedding', $q, 1) YIELD node, score "
                  "MATCH (node)-[:CITES]->(src) RETURN src.title AS cited, score ORDER BY cited", q=[1, 0, 0]).data()
    assert rows == [{"cited": "c", "score": 1.0}, {"cited": "d", "score": 1.0}]
    # created in the same query: pending writes are flushed before the search
    rows = cy.run("CREATE (:Doc {title: 'e', kind: 'note', embedding: [0, 0, 1]}) WITH 1 AS x "
                  "CALL vector.search('Doc', 'embedding', [0, 0, 1], 1) YIELD node RETURN node.title AS t").data()
    assert rows == [{"t": "e"}]
    cy.run("MATCH (d:Doc {title: 'e'}) DETACH DELETE d")                    # deleted nodes are not found
    assert search(cy, "CALL vector.search('Doc', 'embedding', [0, 0, 1], 1) YIELD node RETURN node.title AS t, 1.0 AS score") \
        != [("e", 1.0)]


def test_vector_search_errors():
    cy = engine(DOCS)
    for q, msg in [
        ("CALL vector.search('Doc', 'embedding', [1, 0], 3) YIELD node RETURN node", "not a Vector\\[3\\]"),
        ("CALL vector.search('Doc', 'embedding', [1, 0, 0], 1001) YIELD node RETURN node", "from 1 to 1000"),
        ("CALL vector.search('Doc', 'embedding', [1, 0, 0], 0) YIELD node RETURN node", "from 1 to 1000"),
        ("CALL vector.search('Doc', 'title', [1, 0, 0], 3) YIELD node RETURN node", "is not a Vector field"),
        ("CALL vector.search('Doc', 'unindexed', [1, 0], 3) YIELD node RETURN node", "has no vector index"),
        ("CALL vector.search('Nope', 'embedding', [1, 0, 0], 3) YIELD node RETURN node", "no vertex model"),
        ("CALL vector.search('Doc', 'embedding', [1, 0, 0], 3, {unindexed: [0, 0]}) YIELD node RETURN node",
         "can only filter on indexed fields"),
        ("CALL vector.search('Doc', 'embedding', [1, 0, 0], 3, {kind: 7}) YIELD node RETURN node", "filter kind"),
    ]:
        with pytest.raises(OpenCypherError, match=msg):
            cy.run(q).data()


def test_vector_writes_back_off_to_what_the_server_accepts():
    """Cassandra 5.0 refuses more simultaneous writes to a vector index than its CPU count allows
    ("outstanding pooled objects"); the store halves its concurrency until they go through."""
    from opencypher_graphtool import CassandraStore

    class Refused(Exception):
        pass

    class Limited(CassandraStore):
        limit = 3                                   # a 4-core server's pool
        seen: list = []

        def __init__(self):                         # no session needed for this test
            self.concurrency, self.vector_write_concurrency = 100, 8

        def _concurrent(self, stmt, params, concurrency=None):
            self.seen.append(concurrency)
            over = (concurrency or 100) > self.limit
            return [(False, Refused("Number of outstanding pooled objects has gone beyond the limit of 3"))
                    if over and i >= self.limit else (True, None) for i, _ in enumerate(params)]

    s = Limited()
    s._write_all("stmt", list(range(20)), vector=True)
    assert s.seen == [8, 4, 2]                      # 8 refused some, 4 refused some, 2 went through
    s.seen.clear()

    class Broken(Limited):
        def _concurrent(self, stmt, params, concurrency=None):
            return [(False, Refused("something else")) for _ in params]

    with pytest.raises(Refused, match="something else"):
        Broken()._write_all("stmt", [1], vector=True)    # other errors aren't retried


def test_concurrent_writes_report_each_failure_instead_of_raising(monkeypatch):
    """The driver raises at the first failure unless told otherwise; the store's callers (the
    vector write back-off among them) need each statement's result."""
    pytest.importorskip("cassandra")
    import cassandra.concurrent
    from opencypher_graphtool import CassandraStore
    seen = {}

    def fake(session, stmt, params, **kw):
        seen.update(kw)
        return [(True, None) for _ in params]

    monkeypatch.setattr(cassandra.concurrent, "execute_concurrent_with_args", fake)
    store = CassandraStore.__new__(CassandraStore)
    store.session, store.concurrency = None, 100
    store._concurrent("stmt", [(1,), (2,)], 4)
    assert seen == {"concurrency": 4, "raise_on_first_error": False}
