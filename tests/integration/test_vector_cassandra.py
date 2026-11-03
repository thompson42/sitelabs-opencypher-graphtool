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

"""Vector search against the real docker-cassandra (127.0.0.1:9042), in a throwaway keyspace:
the SAI vector indexes, recall of the approximate search against exact results, score parity with
the engine's vector.* functions, filters, and the migration warnings.

    python -m pytest tests/integration -m integration
"""
import random

import numpy as np
import pytest
from cassandra import ConsistencyLevel
from graph_tool import Graph

import opencypher_graphtool as C
from opencypher_graphtool import CassandraStore, Field, Models, Vector, VectorField, VertexModel, opencypher

pytestmark = pytest.mark.integration

DIM, N, K, QUERIES = 32, 2000, 10, 20
KINDS = ["paper", "note", "talk", "post"]


class VDoc(VertexModel):
    class Meta:
        type = "VDoc"
    title: str = Field(index=True)
    kind: str = Field(index=True)
    e_cos: Vector[DIM] | None = VectorField(similarity="cosine")
    e_dot: Vector[DIM] | None = VectorField(similarity="dot_product")
    e_euc: Vector[DIM] | None = VectorField(similarity="euclidean")


def _unit(v):
    return (v / np.linalg.norm(v)).astype(np.float32)


@pytest.fixture
def loaded(session, keyspace):  # noqa: F811
    session.execute(f"CREATE KEYSPACE \"{keyspace}\" WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}")
    session.cluster.refresh_schema_metadata()
    store = CassandraStore(session, keyspace, read_consistency=ConsistencyLevel.LOCAL_ONE,
                           write_consistency=ConsistencyLevel.LOCAL_QUORUM)
    cy = opencypher(Graph(directed=True), store, models=Models([VDoc]))      # migrate: tables + SAI indexes
    rng = np.random.default_rng(5)
    vecs = np.stack([_unit(rng.normal(size=DIM)) for _ in range(N)])         # unit length: fine for all three
    kinds = [KINDS[i % len(KINDS)] for i in range(N)]
    rows = [{"t": f"d{i}", "k": kinds[i], "e": vecs[i].tolist()} for i in range(N)]
    for i in range(0, N, 500):
        cy.run("UNWIND $rows AS r CREATE (:VDoc {title: r.t, kind: r.k, e_cos: r.e, e_dot: r.e, e_euc: r.e})",
               rows=rows[i:i + 500])
    return cy, store, vecs, kinds


def exact(similarity, vecs, q, k, mask=None):
    scores = np.array([C.graph_functions._vector_score(similarity, v, q) for v in vecs])
    if mask is not None:
        scores = np.where(mask, scores, -1.0)
    return [f"d{i}" for i in np.argsort(-scores)[:k]], scores


def test_vector_indexes_are_sai_with_their_similarity(loaded, session, keyspace):  # noqa: F811
    session.cluster.refresh_schema_metadata()
    idx = C.models._index_details(session.cluster.metadata.keyspaces[keyspace].tables["v_doc"])
    assert all(C.models._is_sai(opts) for _, opts in idx.values()) and set(idx) == {"title", "kind", "e_cos", "e_dot", "e_euc"}
    assert {c: idx[c][1]["similarity_function"] for c in ("e_cos", "e_dot", "e_euc")} == \
        {"e_cos": "COSINE", "e_dot": "DOT_PRODUCT", "e_euc": "EUCLIDEAN"}


@pytest.mark.parametrize("field,similarity", [("e_cos", "cosine"), ("e_dot", "dot_product"), ("e_euc", "euclidean")])
def test_recall_and_scores_against_exact_search(loaded, field, similarity):
    cy, _, vecs, _ = loaded
    rng = np.random.default_rng(9)
    recalls, worst = [], 0.0
    for _ in range(QUERIES):
        q = _unit(rng.normal(size=DIM))
        rows = cy.run(f"CALL vector.search('VDoc', '{field}', $q, {K}) YIELD node, score "
                      f"RETURN node.title AS t, score, vector.{similarity}(node.{field}, $q) AS mine", q=q.tolist()).data()
        want, scores = exact(similarity, vecs, q, K)
        got = [r["t"] for r in rows]
        recalls.append(len(set(got) & set(want)) / K)
        for r in rows:                              # Cassandra's score == the engine's score
            worst = max(worst, abs(r["score"] - r["mine"]), abs(r["score"] - scores[int(r["t"][1:])]))
        assert [r["score"] for r in rows] == sorted((r["score"] for r in rows), reverse=True)
    recall = sum(recalls) / len(recalls)
    print(f"\n  {similarity}: recall@{K} over {QUERIES} queries = {recall:.3f}, largest score difference {worst:.2e}")
    assert recall >= 0.97                           # with the default oversampling (x4): 0.99 measured
    assert worst < 1e-5


def test_filtered_search_and_graph_follow_up(loaded):
    cy, _, vecs, kinds = loaded
    q = _unit(np.random.default_rng(3).normal(size=DIM))
    rows = cy.run("CALL vector.search('VDoc', 'e_cos', $q, 10, {kind: 'talk'}) YIELD node, score "
                  "RETURN node.title AS t, node.kind AS k, score", q=q.tolist()).data()
    assert len(rows) == 10 and {r["k"] for r in rows} == {"talk"}
    want, _ = exact("cosine", vecs, q, 10, mask=np.array([k == "talk" for k in kinds]))
    assert len({r["t"] for r in rows} & set(want)) >= 8
    n = cy.run("CALL vector.search('VDoc', 'e_cos', $q, 5) YIELD node WITH node MATCH (d:VDoc {kind: node.kind}) "
               "RETURN count(d) AS c", q=q.tolist()).value()
    assert n == 5 * N // len(KINDS)                 # each match's kind has N/4 documents


def test_old_style_indexes_are_reported_and_a_changed_similarity_blocked(loaded, session, keyspace):  # noqa: F811
    cy, store, _, _ = loaded
    session.execute(f'CREATE TABLE "{keyspace}".legacy_doc (id uuid PRIMARY KEY, title text)')
    session.execute(f'CREATE INDEX legacy_title ON "{keyspace}".legacy_doc (title)')       # not SAI
    session.cluster.refresh_schema_metadata()

    class LegacyDoc(VertexModel):
        class Meta:
            type = "LegacyDoc"
            table = "legacy_doc"
        title: str = Field(index=True)
    p = Models([LegacyDoc]).plan(store)
    assert p.statements == [] and len(p.warnings) == 1 and "not SAI" in p.warnings[0]

    class VDocEuc(VertexModel):                    # the same table, cosine field now said to be euclidean
        class Meta:
            type = "VDoc"
            table = "v_doc"
        title: str = Field(index=True)
        kind: str = Field(index=True)
        e_cos: Vector[DIM] | None = VectorField(similarity="euclidean")
        e_dot: Vector[DIM] | None = VectorField(similarity="dot_product")
        e_euc: Vector[DIM] | None = VectorField(similarity="euclidean")
    p = Models([VDocEuc]).plan(store)
    assert len(p.blocked) == 1 and "uses COSINE, the model says EUCLIDEAN" in p.blocked[0]
