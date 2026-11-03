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

"""Commit-time validation of rows that a mid-query flush already wrote (MERGE over many rows
flushes so its store searches can see earlier rows)."""
import pytest
from graph_tool import Graph
from pydantic import field_validator

from opencypher_graphtool import ConstraintError, Field, InMemoryStore, Models, VertexModel, opencypher


class Person(VertexModel):
    class Meta:
        type = "Person"
    name: str = Field(index=True)
    address: str | None = None
    status: str = "active"

    @field_validator("name")
    @classmethod
    def not_blank(cls, v):
        if not v.strip():
            raise ValueError("blank name")
        return v


def make():
    store = InMemoryStore({})
    return opencypher(Graph(), store, models=Models([Person])), store


def rows(store):
    return sorted((r.get("name"), r.get("address"), r.get("status"))
                  for r in store.tables[("vertex", "Person")].values())


def test_multi_row_merge_then_set_keeps_the_merged_properties():
    cy, store = make()
    cy.run("UNWIND ['A', 'B', 'C'] AS x MERGE (n:Person {name: x}) SET n.address = 'z'")
    assert rows(store) == [("A", "z", "active"), ("B", "z", "active"), ("C", "z", "active")]


def test_rows_flushed_mid_query_get_defaults_and_are_validated():
    cy, store = make()
    cy.run("UNWIND ['A', 'B'] AS x MERGE (n:Person {name: x})")
    assert rows(store) == [("A", None, "active"), ("B", None, "active")]
    with pytest.raises(ConstraintError, match="blank name"):
        cy.run("UNWIND ['C', ' '] AS x MERGE (n:Person {name: x}) SET n.address = 'q'")
    assert rows(store) == [("A", None, "active"), ("B", None, "active")]      # rolled back
