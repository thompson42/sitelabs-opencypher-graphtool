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

"""openCypher: graph-aware functions (their first argument is the runner)."""
from __future__ import annotations

from graph_tool import Edge
from graph_tool import Graph
from graph_tool import Vertex
from typing import Any
from typing import Callable
from .errors import OpenCypherError
from .values import CPath, _ekey

# --------------------------------------------------------------------------- #
# graph-aware functions (first argument is the _Run)
# --------------------------------------------------------------------------- #
def _gf_id(r, x):
    if x is None: return None
    if not isinstance(x, (Vertex, Edge)): raise OpenCypherError("id() needs a node or relationship")
    return r.sc.get_id(r.g, x)


def _gf_labels(r, x):
    if x is None: return None
    if not isinstance(x, Vertex): raise OpenCypherError("labels() needs a node")
    return r.sc.label_names(r.g, x)


def _gf_type(r, x):
    if x is None: return None
    if not isinstance(x, Edge): raise OpenCypherError("type() needs a relationship")
    return r.sc.type_name(r.g, x)


def _gf_keys(r, x):
    if x is None: return None
    if isinstance(x, dict): return list(x.keys())
    return [k for k, v in r.props_of(x).items()]


def _gf_properties(r, x):
    if x is None: return None
    if isinstance(x, dict): return dict(x)
    if not isinstance(x, (Vertex, Edge)): raise OpenCypherError("properties() needs a node, relationship or map")
    return r.props_of(x)


def _gf_nodes(r, p): return None if p is None else list(p.nodes)
def _gf_rels(r, p): return None if p is None else list(p.rels)
def _gf_length(r, p):
    if p is None: return None
    if isinstance(p, CPath): return len(p.rels)
    raise OpenCypherError("length() needs a path (use size() for lists and strings)")
def _gf_start(r, e): return None if e is None else e.source()
def _gf_end(r, e): return None if e is None else e.target()
def _gf_exists(r, x): return x is not None
def _gf_degree(r, v, direction=None):
    if v is None: return None
    d = (direction or "both").lower()
    return len(_incident(r.g, v, {"out": "out", "in": "in"}.get(d, "all")))
def _gf_elementid(r, x): return _gf_id(r, x)


# similarity name in the models / functions -> Cassandra's similarity_function
VECTOR_SIMILARITIES = {"cosine": "COSINE", "dot_product": "DOT_PRODUCT", "euclidean": "EUCLIDEAN"}


def _vector_score(similarity: str, a: Any, b: Any) -> "float | None":
    """Similarity of two vectors scaled to 0..1 exactly as Cassandra's similarity_* functions:
    cosine and dot_product -> (1 + x) / 2, euclidean -> 1 / (1 + squared distance).
    None if either is null (or, for cosine, a zero vector)."""
    if a is None or b is None:
        return None
    import numpy as np
    x, y = np.asarray(a, dtype=np.float64), np.asarray(b, dtype=np.float64)
    if x.ndim != 1 or x.shape != y.shape:
        raise OpenCypherError(f"vector dimensions differ: {x.size} and {y.size}")
    if similarity == "euclidean":
        return float(1.0 / (1.0 + float(np.dot(x - y, x - y))))
    if similarity == "dot_product":
        return float((1.0 + float(np.dot(x, y))) / 2.0)
    nx, ny = float(np.linalg.norm(x)), float(np.linalg.norm(y))
    if nx == 0.0 or ny == 0.0:
        return None
    return float((1.0 + float(np.dot(x, y)) / (nx * ny)) / 2.0)


VECTOR_SCAN_LIMIT = 100_000   # vector.* evaluations per query (see OpenCypher.vector_scan_limit)


def _gf_vector(similarity: str) -> Callable:
    def fn(run, a, b):
        run.vector_evals += 1
        limit = run.eng.vector_scan_limit
        if limit and run.vector_evals > limit:
            raise OpenCypherError(
                f"this query scores more than {limit} vectors with vector.{similarity}(); to search the "
                f"whole graph use CALL vector.search(type, field, vector, k) instead (it runs in Cassandra)")
        return _vector_score(similarity, a, b)
    return fn


_GRAPH_FUNCS: dict[str, Callable] = {
    "id": _gf_id, "elementid": _gf_elementid, "labels": _gf_labels, "type": _gf_type,
    "keys": _gf_keys, "properties": _gf_properties, "nodes": _gf_nodes,
    "relationships": _gf_rels, "rels": _gf_rels, "length": _gf_length,
    "startnode": _gf_start, "endnode": _gf_end, "exists": _gf_exists, "degree": _gf_degree,
    **{f"vector.{s}": _gf_vector(s) for s in VECTOR_SIMILARITIES},
}


def _incident(g: Graph, v: Vertex, direction: str) -> list:
    if direction == "out":
        return list(v.out_edges())
    if direction == "in":
        return list(v.in_edges())
    seen, out = set(), []
    for e in v.all_edges():
        k = _ekey(g, e)
        if k not in seen:
            seen.add(k); out.append(e)
    return out
