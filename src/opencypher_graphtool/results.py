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

"""openCypher: query results (Node, Rel, Path, Result)."""
from __future__ import annotations

from collections import Counter
from .errors import OpenCypherError
from .values import _opencypher_repr

# =========================================================================== #
# openCypher: results
# =========================================================================== #
class Node:
    """A returned vertex: n["name"], n.labels, n.id (UUID), n.properties."""
    __slots__ = ("id", "labels", "properties")

    def __init__(self, id, labels, properties):
        self.id, self.labels, self.properties = id, labels, properties

    element_id = property(lambda s: s.id)
    def __getitem__(self, k): return self.properties[k]
    def get(self, k, default=None): return self.properties.get(k, default)
    def __contains__(self, k): return k in self.properties
    def keys(self): return self.properties.keys()
    def items(self): return self.properties.items()
    def __eq__(self, o): return isinstance(o, Node) and o.id == self.id
    def __hash__(self): return hash(self.id)
    def __repr__(self):
        lab = "".join(":" + l for l in self.labels)
        return f"({lab} {_opencypher_repr(self.properties)})"


class Rel:
    """A returned relationship: r.type, r.start / r.end (UUIDs), r["weight"]."""
    __slots__ = ("id", "type", "start", "end", "properties")

    def __init__(self, id, type, start, end, properties):
        self.id, self.type, self.start, self.end, self.properties = id, type, start, end, properties

    element_id = property(lambda s: s.id)
    def __getitem__(self, k): return self.properties[k]
    def get(self, k, default=None): return self.properties.get(k, default)
    def keys(self): return self.properties.keys()
    def items(self): return self.properties.items()
    def __eq__(self, o): return isinstance(o, Rel) and o.id == self.id
    def __hash__(self): return hash(self.id)
    def __repr__(self): return f"[:{self.type} {_opencypher_repr(self.properties)}]"


class Path:
    __slots__ = ("nodes", "relationships")

    def __init__(self, nodes, relationships):
        self.nodes, self.relationships = nodes, relationships

    def __len__(self): return len(self.relationships)
    def __eq__(self, o): return isinstance(o, Path) and self.nodes == o.nodes and self.relationships == o.relationships
    def __hash__(self): return hash(tuple(self.nodes))
    def __repr__(self): return f"<Path length={len(self)} nodes={self.nodes!r}>"


class Result:
    """Rows of a query as dicts. Iterate it, or use .data() / .single() / .value() / .values().
    .summary counts what a write query changed (nodes_created, properties_set, ...)."""
    def __init__(self, columns: list, rows: list, summary: Counter, plan: list | None = None):
        self.columns, self.rows, self.summary, self.plan = columns, rows, summary, plan

    def __iter__(self): return iter(self.rows)
    def __len__(self): return len(self.rows)
    def __getitem__(self, i): return self.rows[i]
    def keys(self): return list(self.columns)
    def data(self) -> list[dict]: return list(self.rows)
    def values(self, *cols) -> list[list]:
        cols = cols or self.columns
        return [[r[c] for c in cols] for r in self.rows]

    def single(self) -> dict:
        if len(self.rows) != 1:
            raise OpenCypherError(f"expected exactly one row, got {len(self.rows)}")
        return self.rows[0]

    def value(self, col: str | None = None):
        """The first row's value of `col` (default: first column); None for no rows."""
        if not self.rows:
            return None
        return self.rows[0][col if col is not None else self.columns[0]]

    def __repr__(self):
        return f"Result(columns={self.columns}, rows={len(self.rows)}, summary={dict(self.summary)})"
