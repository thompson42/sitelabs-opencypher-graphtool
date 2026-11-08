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

"""OpenCypher.runner and the neighbourhood() hook: called once per hop with the whole frontier,
once per level of a variable-length pattern, once per BFS layer, and by degree()."""
from graph_tool import Graph

from opencypher_graphtool import OpenCypher
from opencypher_graphtool.clauses import _Run


class Recording(_Run):
    calls: list = []

    def neighbourhood(self, vertices, direction, types=(), labels=()):
        Recording.calls.append((len(vertices), direction, tuple(types), tuple(labels)))


class RecordingOpenCypher(OpenCypher):
    runner = Recording


def make():
    cy = RecordingOpenCypher(Graph())
    cy.run("CREATE (a:P {n: 1})-[:R]->(b:P {n: 2})-[:R]->(c:Q {n: 3}), (a)-[:R]->(c)")
    Recording.calls.clear()
    return cy


def test_one_call_per_hop_with_the_whole_frontier_and_its_filters():
    cy = make()
    assert len(cy.run("MATCH (x:P)-[:R]->(y:Q) RETURN y.n").data()) == 2
    assert Recording.calls == [(1, "in", ("R",), ("P",))]      # from the rarer label, backwards
    Recording.calls.clear()
    assert len(cy.run("MATCH (x:P)-[:R]->(y) RETURN y.n").data()) == 3
    assert Recording.calls == [(2, "out", ("R",), ())]


def test_variable_length_bfs_and_degree():
    cy = make()
    cy.run("MATCH (x {n: 1})-[:R*1..3]->(y) RETURN y.n")
    assert [c[0] for c in Recording.calls] == [1, 2, 1]          # one call per level
    Recording.calls.clear()
    cy.run("MATCH p = shortestPath((x {n: 1})-[*]->(y {n: 3})) RETURN length(p)")
    assert Recording.calls and all(c[1] == "out" for c in Recording.calls)
    Recording.calls.clear()
    assert cy.run("MATCH (x {n: 2}) RETURN degree(x) AS d").value() == 2
    assert Recording.calls == [(1, "all", (), ())]
