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

from opencypher_graphtool import opencypher, CassandraStore, Models, VertexModel, EdgeModel, Field

class Person(VertexModel):
    class Meta:
        type = "Person"                       # or: class Person(VertexModel, table="people")
    name: str = Field(index=True)             # becomes a Cassandra secondary index
    age: int | None = Field(default=None, ge=0)
    status: Status = Status.ACTIVE            # defaults are written when a node is created

class Knows(EdgeModel):
    class Meta:
        type = "KNOWS"
    since: int = Field(ge=1900)               # required on every KNOWS edge

store = CassandraStore(session, "mykeyspace")
cy = opencypher(graph, store, models=Models([Person, Knows]))     # no Schema, no table_namer
