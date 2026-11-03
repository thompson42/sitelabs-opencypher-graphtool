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

"""Creating, changing and deleting graph-tool vertices and edges (with undo for transactions)."""
from __future__ import annotations

from graph_tool import Edge
from graph_tool import Graph
from graph_tool import Vertex
from typing import Any
from .locking import _check_write
from .common import CORE_KEYS, _CHUNKS, _norm, _txn_of
from .values import _refuse_maps
from .schema import Schema, _schema, _store

def _is_v(x): return isinstance(x, Vertex)
def _is_e(x): return isinstance(x, Edge)



def _prop(g: Graph, el: Any, key: Any) -> Any:
    """THE routing point for property reads.

    id (UUID str) / label (lowest-bit label name) / type (type name)
                   -> graph-tool (bitmask + uuid property maps).
    anything else  -> the attached store (or graph-tool property maps if no
                      store is attached).
    """
    k = _norm(key)
    is_v = _is_v(el)
    if not (is_v or _is_e(el)):
        raise TypeError(f"Cannot read property {k!r} of {type(el).__name__}; "
                        "expected a vertex or edge")
    sc = _schema(g)
    if k == "id":
        return sc.get_id(g, el)
    if k == "label":
        return sc.primary_label(g, el)
    if k == "type":
        return sc.type_name(g, el)
    if is_v and k in sc.computed:               # pageRank() & co. results
        return sc.computed[k].get(int(el))
    pm = g.vp if is_v else g.ep
    store = _store(g)
    if store is None:
        return pm[k][el] if k in pm else None
    tn, id_ = sc.type_name(g, el), sc.get_id(g, el)
    if tn is None or id_ is None:
        return None
    return store.get("vertex" if is_v else "edge", tn, id_, k)


def _all_keys(g: Graph, el: Any) -> list:
    """Property names for values()/valueMap() with no arguments (core keys excluded)."""
    store = _store(g)
    sc = _schema(g)
    if store is None:
        keys = list((g.vp if _is_v(el) else g.ep).keys())
        return [k for k in keys if k not in CORE_KEYS and k not in sc.internal_maps]
    tn = _prop(g, el, "type")
    keys = store.columns("vertex" if _is_v(el) else "edge", tn) if tn else []
    return [k for k in keys if k not in CORE_KEYS and k != store.id_column]



def _chunks(stream, sizes: tuple = _CHUNKS):
    """Group a stream into lists of growing size (sizes[0], sizes[1], ..., then the
    last size repeated). Lazy: stops pulling from upstream when the consumer stops."""
    buf: list = []
    i = 0
    limit = sizes[0]
    for t in stream:
        buf.append(t)
        if len(buf) >= limit:
            yield buf
            buf = []
            i += 1
            limit = sizes[min(i, len(sizes) - 1)]
    if buf:
        yield buf



def _prefetch_objs(g: Graph, objs: list) -> None:
    store = _store(g)
    if store is not None:
        for i in range(0, len(objs), 512):
            store.prefetch(g, objs[i:i + 512])



def _alive(g: Graph, v: Vertex) -> bool:
    return not ("_dropped" in g.vp and g.vp["_dropped"][v])



def _undo_created(g: Graph, kind: str, el: Any) -> None:
    """Inverse of _create_vertex/_create_edge (journal entries run newest-first, so a created
    vertex is always the highest index when its turn comes and can really be removed)."""
    sc = _schema(g)
    sc.retire(g, kind, el)
    if kind == "edge":
        g.remove_edge(el)
        return
    rv = getattr(g, "remove_vertex", None)
    if rv is not None and int(el) == g.num_vertices() - 1:
        rv(el)
    else:                                               # cannot remove safely: tombstone
        if "_dropped" not in g.vp:
            g.vp["_dropped"] = g.new_vertex_property("bool")
        g.vp["_dropped"][el] = True


def _undo_edge_delete(g: Graph, sc: "Schema", src, dst, lm: int, tm: int, eid: str) -> None:
    e = g.add_edge(src, dst)
    sc.commit(g, "edge", e, lm, tm, eid)


def _undo_vertex_delete(g: Graph, sc: "Schema", v, lm: int, tm: int, vid: str) -> None:
    g.vp[sc.label_map][v] = sc.to_stored(lm)
    g.vp[sc.type_map][v] = sc.to_stored(tm)
    g.vp[sc.id_map][v] = vid
    sc._index_put("vertex", vid, v)
    g.vp["_dropped"][v] = False
    sc.computed.clear()


def _create_vertex(g: Graph, labels: tuple, type_: str | None, id_: Any = None) -> Vertex:
    _check_write(g)
    sc, store = _schema(g), _store(g)
    labels = tuple(labels) or ("vertex",)
    tn = type_ or labels[0]
    if type_ is None and store is not None and store.models is not None:
        tn = store.models.pick_type(labels) or labels[0]      # (:Admin:Person) -> the Person table
    if store is not None:
        store.validate_type("vertex", tn)       # fail BEFORE touching graph-tool
    lm, tm, vid = sc.prepare("vertex", labels, tn, id_)
    v = g.add_vertex()
    sc.commit(g, "vertex", v, lm, tm, vid)
    t = _txn_of(g)
    if t is not None:
        t.created.add(("vertex", tn, vid))
        t.undo.append(lambda g=g, v=v: _undo_created(g, "vertex", v))
        t.redo.append(["v+", vid, sc.label_names(g, v), tn])
    if store is not None:
        store.stage_write("vertex", tn, vid, {})
    return v


def _create_edge(g: Graph, src: Vertex, dst: Vertex, label: str, type_: str | None,
                 id_: Any = None) -> Edge:
    _check_write(g)
    sc, store = _schema(g), _store(g)
    tn = type_ or label
    if store is not None:
        store.validate_type("edge", tn)
    lm, tm, eid = sc.prepare("edge", (), tn, id_)          # an edge has a type and no labels
    e = g.add_edge(src, dst)
    sc.commit(g, "edge", e, lm, tm, eid)
    t = _txn_of(g)
    if t is not None:
        t.created.add(("edge", tn, eid))
        t.undo.append(lambda g=g, e=e: _undo_created(g, "edge", e))
        t.redo.append(["e+", eid, sc.get_id(g, src), sc.get_id(g, dst), tn])
    if store is not None:
        store.stage_write("edge", tn, eid, {})
    return e


def _set_props(g: Graph, el: Any, props: dict) -> None:
    _check_write(g)
    is_v = _is_v(el)
    if not (is_v or _is_e(el)):
        raise TypeError(f"property() needs a vertex or edge, got {type(el).__name__}")
    props = {_norm(k): v for k, v in props.items()}
    for k in props:
        if k in CORE_KEYS:
            raise ValueError(f"{k!r} is a core key: set it via addV/addE(label, type=...)")
    store = _store(g)
    if store is None:                              # plain graph-tool property maps
        _refuse_maps(props, lambda k: False)
        pms = g.vp if is_v else g.ep
        t = _txn_of(g)
        for k, v in props.items():
            if k not in pms:
                pms[k] = (g.new_vertex_property if is_v else g.new_edge_property)("object")
            elif t is not None:
                t.undo.append(lambda pm=pms[k], el=el, old=pms[k][el]: pm.__setitem__(el, old))
            pms[k][el] = v
        return
    kind = "vertex" if is_v else "edge"
    tn, id_ = _prop(g, el, "type"), _prop(g, el, "id")
    if not tn:
        raise ValueError("element has no 'type', so no table to write to")
    if id_ is None:
        raise ValueError("element has no id (see Schema.assign_missing_ids)")
    store.validate_columns(kind, tn, list(props))
    props = store.coerce_props(kind, tn, props)      # pydantic: type-check / convert values
    store.stage_write(kind, tn, id_, props)



class _SemiJoin:
    """Per-run memo for one has(key, value) step: for each (kind, type) table the
    set of ids whose row matches, fetched with ONE id-only indexed query."""

    def __init__(self, store: "PropertyStore", key: str, values: list):
        self.store, self.key, self.values = store, key, values
        self.sets: dict[tuple, set | None] = {}

    def verdict(self, g: Graph, sc: "Schema", el: Any):
        """True / False when the semi-join decides this element, None to fetch its row."""
        if not (_is_v(el) or _is_e(el)):
            return None
        kind = "vertex" if _is_v(el) else "edge"
        tn, id_ = sc.type_name(g, el), sc.get_id(g, el)
        if tn is None or id_ is None:
            return None
        if self.key in sc.computed:
            return None                                 # algorithm output: decided by _prop
        ck = (kind, tn)
        if ck not in self.sets:
            if self.store.has_pending():
                self.store.flush()                      # searches only see flushed rows
            ids = self.store.semijoin_ids(kind, tn, self.key, self.values,
                                          self.store.semijoin_limit)
            self.sets[ck] = ids
            self.store.stats["semijoins" if ids is not None else "semijoin_fallbacks"] += 1
        ids = self.sets[ck]
        return None if ids is None else (id_ in ids)



def _alive_mask(g: Graph):
    """numpy bool per vertex index: False for tombstoned (dropped) vertices."""
    import numpy as np
    n = g.num_vertices()
    if "_dropped" in g.vp:
        return ~np.asarray(g.vp["_dropped"].a[:n], dtype=bool)
    return np.ones(n, dtype=bool)



def _edge_weights(g: Graph, key: str, what: str, strict: bool, only: "set | None" = None):
    """Edge property map (double) of the stored/graph-tool property `key`. Reads every
    edge once (batched store prefetch). strict: missing/negative values raise."""
    edges = [e for e in g.edges() if only is None or int(g.edge_index[e]) in only]
    w = g.new_edge_property("double")
    for i in range(0, len(edges), 500):
        batch = edges[i:i + 500]
        _prefetch_objs(g, batch)
        for e in batch:
            x = _prop(g, e, key)
            if x is None:
                if strict:
                    raise ValueError(f"{what}: edge {_prop(g, e, 'id')} has no {key!r} value")
                x = 1.0
            x = float(x)
            if strict and x < 0:
                raise ValueError(f"{what}: negative {key!r} on edge {_prop(g, e, 'id')}")
            w[e] = x
    return w


def _link() -> None:
    """Bind the names this module uses from modules loaded after it (they import this one).
    Called once by the package __init__, after every module is loaded."""
    global PropertyStore
    from .stores import PropertyStore
