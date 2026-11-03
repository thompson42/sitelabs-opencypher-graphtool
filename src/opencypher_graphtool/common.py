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

"""Small shared pieces: the write transaction record, sentinels, core keys."""
from __future__ import annotations

from enum import Enum
from graph_tool import Graph
from typing import Any

_TXN_ATTR = "_gt_txn"


class _Txn:
    """Undo journal of ONE write query. It lives only while the query holds the exclusive graph
    lock, so no other thread ever sees the uncommitted state. `undo` holds inverse graph-tool
    operations (run LIFO on failure); the store side keeps pre-images so rows that were already
    written can be put back (compensation)."""

    def __init__(self, graph: Graph, store: Any):
        self.g, self.store = graph, store
        self.undo: list = []        # callables, undone in reverse order
        self.created: set = set()   # (kind, type, id) created by this query: pre-image = absent
        self.pre: dict = {}         # (kind, type, id) -> row before this query, or None if absent
        self.cols: dict = {}        # (kind, type, id) -> columns this query wrote
        self.attempted: set = set() # keys sent to the store (may be partially applied)
        self.applied: set = set()   # keys whose store call RETURNED: certainly written
        self.redo: list = []        # id-based topology ops of this query (see OpenCypher.on_commit)
        self.written: dict = {}     # (kind, type, id) -> values flushed so far by this query


def _txn_of(g: Graph) -> "_Txn | None":
    return g.__dict__.get(_TXN_ATTR)


_UNSET = object()
_CHUNKS = (32, 128, 512)   # growing prefetch chunks: cheap for limit(), big for scans
OPTIMIZE_FILTERS = True    # reorder adjacent pure filters cheap-first


CORE_KEYS = frozenset({"id", "label", "type"})  # always served by graph-tool



def _norm(key: Any) -> Any:
    return key.value if isinstance(key, T) else key



class T(Enum):
    id = "id"
    label = "label"
    type = "type"
