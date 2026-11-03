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

"""openCypher: procedures (CALL ...)."""
from __future__ import annotations

from graph_tool import Graph
from graph_tool import Vertex
from typing import Any
from typing import Callable
from .errors import OpenCypherError
from .common import CORE_KEYS
from .values import CPath
from .models import _ValidationError, _to_store
from .schema import Schema
from .elements import _alive, _alive_mask, _edge_weights

# =========================================================================== #
# openCypher: procedures (CALL ...)
# =========================================================================== #
def _cy_view(g: Graph, sc: Schema, mask, direction: str, types: tuple = ()):
    """GraphView hiding dropped vertices (and edges not of `types`); direction out|in|both."""
    import numpy as np
    from graph_tool import GraphView
    vf = g.new_vertex_property("bool")
    vf.a[:] = mask
    kw = {}
    if types:
        ef = g.new_edge_property("bool")
        keep = np.zeros(len(ef.a), dtype=bool)
        keep[sc.candidates(g, "edge", [("type", tuple(types), "any")])] = True
        ef.a[:] = keep
        kw["efilt"] = ef
    return GraphView(g, vfilt=vf, directed=(False if direction == "both" else None),
                     reversed=(direction == "in"), **kw)


def _type_idx(g: Graph, sc: Schema, cfg: dict):
    """Edge indices allowed by cfg["relationshipTypes"] (None = all edges)."""
    types = tuple(cfg.get("relationshipTypes") or ())
    return set(sc.candidates(g, "edge", [("type", types, "any")]).tolist()) if types else None


def _dir_arg(v, default="out") -> str:
    d = default if v is None else str(v).lower()
    d = {"outgoing": "out", "incoming": "in", "undirected": "both"}.get(d, d)
    if d not in ("out", "in", "both"):
        raise OpenCypherError(f"direction must be OUT, IN or BOTH, got {v!r}")
    return d


def _cfg(c: Any) -> dict:
    if c is None:
        return {}
    if not isinstance(c, dict):
        raise OpenCypherError("procedure configuration must be a map")
    return c


def _proc_labels(run, *a):
    return ["label"], [{"label": n} for n in run.sc.reg("vertex", "label").all_names()]


def _proc_reltypes(run, *a):
    return ["relationshipType"], [{"relationshipType": n} for n in run.sc.reg("edge", "type").all_names()]


def _proc_propkeys(run, *a):
    keys: list = []
    if run.store is not None:
        for kind in ("vertex", "edge"):
            for tn in run.sc.reg(kind, "type").all_names():
                try:
                    for k in run.store.columns(kind, tn):
                        if k not in CORE_KEYS and k != run.store.id_column and k not in keys:
                            keys.append(k)
                except KeyError:
                    pass
    return ["propertyKey"], [{"propertyKey": k} for k in keys]


def _proc_pagerank(run, config=None):
    import numpy as np
    from graph_tool.centrality import pagerank
    c = _cfg(config)
    g, sc = run.g, run.sc
    mask = _alive_mask(g)
    view = _cy_view(g, sc, mask, _dir_arg(c.get("direction")), tuple(c.get("relationshipTypes") or ()))
    wkey = c.get("weightProperty")
    w = _edge_weights(g, wkey, "gt.pageRank", False, _type_idx(g, sc, c)) if wkey else None
    r = pagerank(view, damping=c.get("dampingFactor", 0.85), weight=w,
                 epsilon=c.get("tolerance", 1e-6), max_iter=c.get("maxIterations"))
    arr = np.asarray(r.a)
    rows = [{"node": g.vertex(int(i)), "score": float(arr[i])} for i in np.flatnonzero(mask)]
    return ["node", "score"], rows


def _proc_components(run, config=None):
    import numpy as np
    from graph_tool.topology import label_components
    c = _cfg(config)
    g, sc = run.g, run.sc
    mask = _alive_mask(g)
    view = _cy_view(g, sc, mask, "both", tuple(c.get("relationshipTypes") or ()))
    comp, _ = label_components(view, directed=False)
    alive = np.flatnonzero(mask)
    labels = np.asarray(comp.a)[alive]
    _, first = np.unique(labels, return_index=True)
    rep = {int(labels[f]): sc.get_id(g, g.vertex(int(alive[f]))) for f in first}
    rows = [{"node": g.vertex(int(i)), "componentId": rep[int(l)]} for i, l in zip(alive, labels)]
    return ["node", "componentId"], rows


def _proc_peer(run, config=None):
    import numpy as np
    c = _cfg(config)
    g, sc = run.g, run.sc
    d = _dir_arg(c.get("direction"), "both")
    mask = _alive_mask(g)
    n = len(mask)
    e = np.asarray(g.get_edges(), dtype=np.int64).reshape(-1, 2)
    if c.get("relationshipTypes") and len(e):
        types = tuple(c["relationshipTypes"])
        okidx = set(sc.candidates(g, "edge", [("type", types, "any")]).tolist())
        keep = np.array([int(g.edge_index[ed]) in okidx for ed in g.edges()], dtype=bool)
        e = e[keep] if len(keep) == len(e) else e
    if len(e):
        e = e[mask[e[:, 0]] & mask[e[:, 1]]]
    send, recv = {"out": (e[:, 0], e[:, 1]), "in": (e[:, 1], e[:, 0]),
                  "both": (np.r_[e[:, 0], e[:, 1]], np.r_[e[:, 1], e[:, 0]])}[d]
    alive = np.flatnonzero(mask)
    send, recv = np.r_[send, alive], np.r_[recv, alive]       # own label counts as one vote
    lab = np.arange(n, dtype=np.int64)
    for _ in range(int(c.get("maxIterations", 30))):
        key, cnt = np.unique(recv * n + lab[send], return_counts=True)
        v, l = key // n, key % n
        order = np.lexsort((l, -cnt, v))
        v, l = v[order], l[order]
        first = np.r_[True, v[1:] != v[:-1]]
        new = lab.copy()
        new[v[first]] = l[first]
        if np.array_equal(new, lab):
            break
        lab = new
    low: dict = {}
    for i in alive:
        low.setdefault(int(lab[i]), int(i))
    ids = {k: sc.get_id(g, g.vertex(i)) for k, i in low.items()}
    rows = [{"node": g.vertex(int(i)), "cluster": ids[int(lab[i])]} for i in alive]
    return ["node", "cluster"], rows


def _proc_shortest(run, source, target, config=None):
    from graph_tool.topology import shortest_path
    c = _cfg(config)
    g, sc = run.g, run.sc
    if source is None or target is None:
        return ["path", "totalCost"], []
    if not (isinstance(source, Vertex) and isinstance(target, Vertex)):
        raise OpenCypherError("gt.shortestPath(source, target, config) needs two nodes")
    mask = _alive_mask(g)
    view = _cy_view(g, sc, mask, _dir_arg(c.get("direction")), tuple(c.get("relationshipTypes") or ()))
    wkey = c.get("weightProperty")
    w = _edge_weights(g, wkey, "gt.shortestPath", True, _type_idx(g, sc, c)) if wkey else None
    if int(source) == int(target):
        return ["path", "totalCost"], [{"path": CPath((source,), ()), "totalCost": 0.0}]
    vl, el = shortest_path(view, view.vertex(int(source)), view.vertex(int(target)), weights=w)
    if len(vl) == 0:
        return ["path", "totalCost"], []
    cost = float(sum(w[e] for e in el)) if w is not None else float(len(el))
    return ["path", "totalCost"], [{"path": CPath(tuple(g.vertex(int(v)) for v in vl), tuple(el)),
                                    "totalCost": cost}]


VECTOR_SEARCH_MAX_K = 1000   # Cassandra's limit for ORDER BY ... ANN OF
VECTOR_SEARCH_OVERSAMPLE = 4  # ask for k x this many candidates, keep the best k (recall: see docs)


def _proc_vector_search(run, type_name=None, field=None, vector=None, k=None, filters=None):
    """CALL vector.search(type, field, vector, k [, {filters}]) YIELD node, score

    The k nodes of `type` whose `field` is most similar to `vector`, best first, found by the
    store (Cassandra: an approximate nearest-neighbour search on the field's vector index).
    `filters`: equality conditions on SAI-indexed fields, applied inside the store.
    The store is asked for k x `vector_oversample` candidates (at most 1000) and the k best by
    exact score are kept: the approximate index then misses far fewer true neighbours."""
    if not isinstance(type_name, str) or not isinstance(field, str):
        raise OpenCypherError("vector.search(type, field, vector, k [, filters]): type and field are strings")
    if vector is None:
        return ["node", "score"], []
    if isinstance(k, bool) or not isinstance(k, int) or not 1 <= k <= VECTOR_SEARCH_MAX_K:
        raise OpenCypherError(f"vector.search: k must be an integer from 1 to {VECTOR_SEARCH_MAX_K}, got {k!r}")
    store = run.store
    models = getattr(store, "models", None)
    info = models._by.get(("vertex", type_name)) if models is not None else None
    if info is None:
        raise OpenCypherError(f"vector.search: no vertex model {type_name!r} (vector search needs the data model)")
    f = info.fields.get(field)
    if f is None or f.dim is None:
        raise OpenCypherError(f"vector.search: {info.cls.__name__}.{field} is not a Vector field")
    if not f.index:
        raise OpenCypherError(f"vector.search: {info.cls.__name__}.{field} has no vector index (use VectorField)")
    try:
        vec = f.adapter().validate_python(vector)
    except _ValidationError as ex:
        raise OpenCypherError(f"vector.search: the query vector is not a Vector[{f.dim}]: "
                              + "; ".join(e["msg"] for e in ex.errors())) from None
    conds = {}
    for c, val in _cfg(filters).items():
        fc = info.fields.get(c)
        if fc is None or not fc.index or fc.dim is not None:
            raise OpenCypherError(f"vector.search: can only filter on indexed fields; {info.cls.__name__}.{c} isn't one")
        if val is None:
            return ["node", "score"], []
        try:
            conds[c] = _to_store(fc.adapter().validate_python(val))
        except _ValidationError as ex:
            raise OpenCypherError(f"vector.search: filter {c}: " + "; ".join(e["msg"] for e in ex.errors())) from None
    if store.has_pending():
        store.flush()                                  # the search only sees flushed rows
    want = min(VECTOR_SEARCH_MAX_K, k * max(1, run.eng.vector_oversample or 1))
    hits = store.vector_search("vertex", type_name, field, [float(x) for x in vec], want, conds, f.similarity)
    hits.sort(key=lambda h: -h[1])
    g, sc = run.g, run.sc
    sc.prefetch_ids("vertex", [i for i, _ in hits])
    rows = []
    for id_, score in hits:
        try:
            v = sc.vertex_by_id(g, id_)
        except ValueError:
            continue
        if v is not None and _alive(g, v) and sc.type_name(g, v) == type_name:
            rows.append({"node": v, "score": score})
            if len(rows) == k:
                break
    run.stats["vector_search"] += 1
    return ["node", "score"], rows


_PROCS: dict[str, Callable] = {
    "db.labels": _proc_labels, "db.relationshiptypes": _proc_reltypes,
    "db.propertykeys": _proc_propkeys, "gt.pagerank": _proc_pagerank,
    "gt.connectedcomponents": _proc_components, "gt.peerpressure": _proc_peer,
    "gt.shortestpath": _proc_shortest, "vector.search": _proc_vector_search,
}
_PROC_FIELDS = {
    "db.labels": ("label",), "db.relationshiptypes": ("relationshipType",),
    "db.propertykeys": ("propertyKey",), "gt.pagerank": ("node", "score"),
    "gt.connectedcomponents": ("node", "componentId"), "gt.peerpressure": ("node", "cluster"),
    "gt.shortestpath": ("path", "totalCost"), "vector.search": ("node", "score"),
}
