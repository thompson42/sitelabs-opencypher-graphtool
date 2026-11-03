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

"""Schema: UUID ids and bitmask labels / types, attached to a graph-tool Graph."""
from __future__ import annotations

import json
import warnings
from graph_tool import Graph
from typing import Any
from typing import Iterable
from .ids import new_id, norm_id

# --------------------------------------------------------------------------- #
# Property stores
# --------------------------------------------------------------------------- #
_STORES: dict[int, tuple[Graph, "PropertyStore"]] = {}  # id(graph) -> (graph, store)


_SCHEMAS: dict[int, tuple[Graph, "Schema"]] = {}  # id(graph) -> (graph, schema)


def _schema(g: Graph) -> "Schema":
    entry = _SCHEMAS.get(id(g))
    if entry is None:
        raise RuntimeError("graph is not bound: create it through traversal(graph, ...)")
    return entry[1]


def _store(g: Graph) -> "PropertyStore | None":
    entry = _STORES.get(id(g))
    return entry[1] if entry else None


class Registry:
    """name <-> bit position for one namespace (e.g. vertex labels).

    spec: list of names (bits assigned in order) or {name: bit_position}.
    """

    def __init__(self, spec=None, width: int = 64, auto: bool = True, what: str = "label"):
        self.width, self.auto, self.what = width, auto, what
        self._pos: dict[str, int] = {}
        self._name: dict[int, str] = {}
        if isinstance(spec, dict):
            for n, p in spec.items():
                self._set(n, p)
        else:
            for n in (spec or ()):
                self.register(n)

    def _set(self, name: str, pos: int) -> None:
        if not isinstance(name, str) or not name:
            raise ValueError(f"{self.what} names must be non-empty strings, got {name!r}")
        if isinstance(pos, bool) or not isinstance(pos, int) or not 0 <= pos < self.width:
            raise ValueError(f"{self.what} {name!r}: bit position must be an int in "
                             f"0..{self.width - 1}, got {pos!r}")
        if self._name.get(pos, name) != name or self._pos.get(name, pos) != pos:
            raise ValueError(f"{self.what} {name!r} / bit {pos} conflicts with an existing entry")
        self._pos[name], self._name[pos] = pos, name

    def register(self, name: str) -> int:
        if name in self._pos:
            return self._pos[name]
        for p in range(self.width):
            if p not in self._name:
                self._set(name, p)
                return p
        raise ValueError(f"all {self.width} {self.what} bits are in use; cannot add {name!r}")

    def resolve(self, name: str) -> int:
        """Mask for WRITING a name (registers it when auto-registration is on)."""
        if name in self._pos:
            return 1 << self._pos[name]
        if not self.auto:
            raise KeyError(f"unknown {self.what} {name!r} (auto_register is off)")
        return 1 << self.register(name)

    def lookup(self, name: str) -> int:
        """Mask for READING a name; 0 if the name is unknown."""
        p = self._pos.get(name)
        return 0 if p is None else 1 << p

    def names(self, mask: int) -> list[str]:
        return [self._name[p] for p in sorted(self._name) if (mask >> p) & 1]

    def all_names(self) -> list[str]:
        return [self._name[p] for p in sorted(self._name)]

    def to_dict(self) -> dict:
        return dict(self._pos)


class Schema:
    """Configuration + runtime helpers for ids, labels and types. One per graph.

    vertex_labels / vertex_types / edge_types:
        list of names, or {name: bit position}.   (three independent namespaces)
    A vertex has ANY NUMBER of labels (a bitmask) but exactly ONE type (its table). An edge has
    exactly ONE type (its relationship type, its table) and NO labels.
    bits: 32 or 64 -> int32/int64 property maps; that many names per namespace.
    label_map / type_map / id_map: names of the internal property maps.
    """

    ATTACH_KEY = "gremlin_gt_schema"     # the graph property saved graph files use: kept for compatibility

    def __init__(self, vertex_labels=None, vertex_types=None, edge_types=None,
                 bits: int = 64, auto_register: bool = True,
                 label_map: str = "labels", type_map: str = "type", id_map: str = "uuid",
                 edge_labels=None):
        if edge_labels is not None:
            raise ValueError("edges have exactly one type and no labels: pass edge_types=... "
                             "(the relationship type) instead of edge_labels")
        if bits not in (32, 64):
            raise ValueError("bits must be 32 or 64")
        self.bits = bits
        self.dtype = "int32_t" if bits == 32 else "int64_t"
        self.auto_register = auto_register
        self.label_map, self.type_map, self.id_map = label_map, type_map, id_map
        mk = lambda spec, what: Registry(spec, bits, auto_register, what)
        self._reg = {
            ("vertex", "label"): mk(vertex_labels, "vertex label"),
            ("vertex", "type"): mk(vertex_types, "vertex type"),
            ("edge", "type"): mk(edge_types, "edge type"),
        }
        self._graph = None
        self.computed: dict[str, dict] = {}  # algorithm results: name -> {vertex index: value}
        self._vidx: Any = {}                # uuid -> vertex index   (dict, or _StoreIdIndex)
        self._eidx: Any = {}                # uuid -> Edge
        self._id_mode = "memory"            # "memory": dict built at bind | "store": reverse table
        self._store: Any = None
        self.stale_hits = 0                 # reverse-index rows found out of date (store mode)

    # -- registries / stored form ------------------------------------------- #
    def reg(self, kind: str, what: str) -> Registry:
        if kind == "edge" and what == "label":
            what = "type"                    # an edge has no labels: its "label" IS its type
        return self._reg[(kind, what)]

    @property
    def internal_maps(self) -> set:
        return {self.label_map, self.type_map, self.id_map, "_dropped"}

    def to_stored(self, mask: int) -> int:
        """Unsigned mask -> signed value that fits the int32/int64 property map."""
        return mask - (1 << self.bits) if (mask >> (self.bits - 1)) & 1 else mask

    def from_stored(self, value: Any) -> int:
        return int(value) & ((1 << self.bits) - 1)

    # -- persistence ---------------------------------------------------------- #
    def to_dict(self) -> dict:
        return {"bits": self.bits, "auto_register": self.auto_register,
                "label_map": self.label_map, "type_map": self.type_map, "id_map": self.id_map,
                "vertex_labels": self.reg("vertex", "label").to_dict(),
                "vertex_types": self.reg("vertex", "type").to_dict(),
                "edge_types": self.reg("edge", "type").to_dict()}

    @classmethod
    def from_dict(cls, d: dict) -> "Schema":
        return cls(vertex_labels=d.get("vertex_labels"),      # (old files may carry "edge_labels": ignored)
                   vertex_types=d.get("vertex_types"), edge_types=d.get("edge_types"),
                   bits=d.get("bits", 64), auto_register=d.get("auto_register", True),
                   label_map=d.get("label_map", "labels"), type_map=d.get("type_map", "type"),
                   id_map=d.get("id_map", "uuid"))

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True)

    @classmethod
    def from_json(cls, s: str) -> "Schema":
        return cls.from_dict(json.loads(s))

    def attach(self, g: Graph) -> None:
        """Store the mapping inside the graph (a graph property) so saved graphs
        are self-describing. Call again after new names are auto-registered.
        (Uses graph-tool's graph-property API; not exercised against real graph-tool.)"""
        g.gp[self.ATTACH_KEY] = g.new_graph_property("string", self.to_json())

    @classmethod
    def from_graph(cls, g: Graph) -> "Schema | None":
        try:
            val = g.gp[cls.ATTACH_KEY]
            if not isinstance(val, str):
                val = val[g]
            return cls.from_json(val)
        except (KeyError, TypeError, ValueError):
            return None

    # -- binding -------------------------------------------------------------- #
    def bind(self, g: Graph, store: Any = None, id_index: str | None = None) -> None:
        """Create/validate the internal property maps and set up the uuid index.

        id_index "memory" (default): one pass over every vertex and edge builds a dict (O(V+E)).
        id_index "store": ids are looked up in the store's reverse table, nothing is scanned
        (needs `store`; the table is built once from the graph if it does not exist yet).
        None keeps the current mode."""
        if self._graph is not None and self._graph is not g:
            raise ValueError("this Schema is already bound to another graph")
        self._graph = g
        for kind, pms, new in (("vertex", g.vp, g.new_vertex_property),
                               ("edge", g.ep, g.new_edge_property)):
            for name, dtype in (((self.label_map, self.dtype),) if kind == "vertex" else ()) + (
                    (self.type_map, self.dtype), (self.id_map, "string")):
                if name not in pms:
                    pms[name] = new(dtype)
                    continue
                vt = getattr(pms[name], "value_type", None)
                vt = vt() if callable(vt) else None
                if vt is not None and ((dtype == "string") != (vt == "string")):
                    raise ValueError(
                        f"existing {kind} property map {name!r} has type {vt!r}; expected "
                        f"{dtype!r}. Labels/types are bitmask ints and ids are UUID strings "
                        "now -- rebuild or convert it.")
        if id_index is not None:
            if id_index not in ("memory", "store"):
                raise ValueError("id_index must be 'memory' or 'store'")
            self._id_mode = id_index
        if store is not None:
            self._store = store
        if self._id_mode == "store":
            if self._store is None:
                raise ValueError("id_index='store' needs a property store")
            try:
                self._store.validate_type("reverse", "ids")
            except KeyError as ex:
                raise ValueError(f"{ex.args[0]} -- create the reverse id table first "
                                 "(CassandraStore.create_reverse_table())") from None
            self._vidx = _StoreIdIndex(self, "vertex", self._store)
            self._eidx = _StoreIdIndex(self, "edge", self._store)
            if not self._store.row("reverse", "ids", _REV_MARKER):
                if g.num_vertices() + g.num_edges() == 0:
                    self._store.write_rows("reverse", "ids", [(_REV_MARKER, {"kind": "m", "idx": 1})])
                    self._store.clear_cache()
                else:
                    warnings.warn("the reverse id table is empty: building it from the graph now "
                                  "(one-time, O(V+E)).", RuntimeWarning, stacklevel=2)
                    self.rebuild_reverse(g, self._store)
            return
        self._vidx, self._eidx = {}, {}
        vpm, epm = g.vp[self.id_map], g.ep[self.id_map]
        for v in g.vertices():
            i = vpm[v]
            if i:
                self._index_put("vertex", norm_id(i), v)
        for e in g.edges():
            i = epm[e]
            if i:
                self._index_put("edge", norm_id(i), e)

    def assign_missing_ids(self, g: Graph) -> int:
        """Give every element that has no id a fresh UUID. Returns how many."""
        n = 0
        for kind, els in (("vertex", g.vertices()), ("edge", g.edges())):
            pm = (g.vp if kind == "vertex" else g.ep)[self.id_map]
            for el in els:
                if not pm[el]:
                    i = new_id()
                    pm[el] = i
                    self._index_put(kind, i, el)
                    n += 1
        return n

    def set_computed(self, name: str, values: dict) -> None:
        """Expose per-vertex algorithm output (pageRank, ...) as a readable property."""
        self.computed[name] = values

    def clear_computed(self) -> None:
        self.computed.clear()

    # -- id index -------------------------------------------------------------- #
    def _index_put(self, kind: str, id_: str, el: Any) -> None:
        d = self._vidx if kind == "vertex" else self._eidx
        if getattr(d, "external", False):
            d[id_] = el                        # duplicates were ruled out in prepare()
            return
        if id_ in d:
            raise ValueError(f"duplicate {kind} id {id_}")
        d[id_] = int(el) if kind == "vertex" else el

    def prefetch_ids(self, kind: str, ids: Iterable[Any]) -> None:
        """Warm the id index for many ids with ONE round trip (store mode; no-op otherwise)."""
        d = self._vidx if kind == "vertex" else self._eidx
        if getattr(d, "external", False):
            ok = []
            for i in ids:
                try:
                    ok.append(norm_id(i))
                except ValueError:
                    pass
            d.prefetch(ok)

    def rebuild_reverse(self, g: Graph, store: Any) -> dict:
        """(Re)build the reverse id table from graph-tool: write a row for every live vertex and
        edge, delete rows whose id is not in the graph, then (re)write the completeness marker.
        One pass, O(V+E); this is the bootstrap/migration step and the repair for a damaged table."""
        out = {"vertices": 0, "edges": 0, "orphans_deleted": 0}
        plural = {"vertex": "vertices", "edge": "edges"}
        seen: set = set()
        for kind in ("vertex", "edge"):
            pm = (g.vp if kind == "vertex" else g.ep)[self.id_map]
            batch: list = []
            for el in (g.vertices() if kind == "vertex" else g.edges()):
                i = pm[el]
                if not i:
                    continue
                i = norm_id(i)
                seen.add(i)
                if kind == "vertex":
                    batch.append((i, {"kind": "v", "idx": int(el)}))
                else:
                    batch.append((i, {"kind": "e", "idx": int(g.edge_index[el]),
                                      "src": int(el.source()), "dst": int(el.target())}))
                if len(batch) >= 500:
                    store.write_rows("reverse", "ids", batch)
                    out[plural[kind]] += len(batch)
                    batch = []
            if batch:
                store.write_rows("reverse", "ids", batch)
                out[plural[kind]] += len(batch)
        stale = [x for x in (_norm_or_raw(i) for i in store.scan_ids("reverse", "ids"))
                 if x not in seen and x != _REV_MARKER]
        for k in range(0, len(stale), 500):
            store.delete_rows("reverse", "ids", stale[k:k + 500])
        out["orphans_deleted"] = len(stale)
        store.write_rows("reverse", "ids", [(_REV_MARKER, {"kind": "m", "idx": 1})])
        for ck in [k for k in store._cache if k[0] == "reverse"]:
            store._cache.pop(ck, None)
        for d in (self._vidx, self._eidx):
            if hasattr(d, "clear") and getattr(d, "external", False):
                d.clear()
        return out

    def vertex_by_id(self, g: Graph, id_: Any):
        i = self._vidx.get(norm_id(id_))
        return None if i is None else g.vertex(i)

    def edge_by_id(self, id_: Any):
        return self._eidx.get(norm_id(id_))

    # -- per-element access ------------------------------------------------------ #
    @staticmethod
    def kind_of(el: Any) -> str:
        if _is_v(el):
            return "vertex"
        if _is_e(el):
            return "edge"
        raise TypeError(f"expected a vertex or edge, got {type(el).__name__}")

    def _pm(self, g: Graph, el: Any, name: str):
        return (g.vp if _is_v(el) else g.ep)[name]

    def get_id(self, g: Graph, el: Any):
        return self._pm(g, el, self.id_map)[el] or None

    def label_mask(self, g: Graph, el: Any) -> int:
        if _is_e(el):                        # edges have no labels: the "label" is the type
            return self.type_mask(g, el)
        return self.from_stored(self._pm(g, el, self.label_map)[el])

    def type_mask(self, g: Graph, el: Any) -> int:
        return self.from_stored(self._pm(g, el, self.type_map)[el])

    def label_names(self, g: Graph, el: Any) -> list[str]:
        return self.reg(self.kind_of(el), "label").names(self.label_mask(g, el))

    def primary_label(self, g: Graph, el: Any):
        n = self.label_names(g, el)
        return n[0] if n else None

    def type_name(self, g: Graph, el: Any):
        n = self.reg(self.kind_of(el), "type").names(self.type_mask(g, el))
        return n[0] if n else None

    # -- filters --------------------------------------------------------------------- #
    def want(self, kind: str, what: str, names: Iterable[str], mode: str):
        """-> (mask, possible). mode 'any': matches if the element has any name;
        'all': has every name. possible=False means nothing can match."""
        reg = self.reg(kind, what)
        m, missing = 0, False
        for n in names:
            b = reg.lookup(n)
            if b:
                m |= b
            else:
                missing = True
        ok = m != 0 and not (mode == "all" and missing)
        return m, ok

    @staticmethod
    def test(current: int, m: int, mode: str) -> bool:
        return (current & m) != 0 if mode == "any" else (current & m) == m

    def has_label(self, g: Graph, el: Any, names, mode: str = "any") -> bool:
        m, ok = self.want(self.kind_of(el), "label", names, mode)
        return ok and self.test(self.label_mask(g, el), m, mode)

    def has_type(self, g: Graph, el: Any, names, mode: str = "any") -> bool:
        m, ok = self.want(self.kind_of(el), "type", names, mode)
        return ok and self.test(self.type_mask(g, el), m, mode)

    def candidates(self, g: Graph, kind: str, constraints: list):
        """Vectorised scan: numpy indices of elements satisfying ALL constraints
        [(what, names, mode)], computed with bit tests over the whole map."""
        import numpy as np
        ok = None
        for what, names, mode in constraints:
            m, possible = self.want(kind, what, names, mode)
            if not possible:
                return np.zeros(0, dtype=np.int64)
            pm = (g.vp if kind == "vertex" else g.ep)[
                self.label_map if (what == "label" and kind == "vertex") else self.type_map]
            a = np.asarray(pm.a)
            ms = a.dtype.type(self.to_stored(m))
            hit = ((a & ms) != 0) if mode == "any" else ((a & ms) == ms)
            ok = hit if ok is None else (ok & hit)
        return np.nonzero(ok)[0]

    # -- create / retire ----------------------------------------------------------------- #
    def prepare(self, kind: str, labels: tuple, type_name: str, id_: Any):
        """Resolve everything that can fail BEFORE graph-tool is mutated."""
        treg = self.reg(kind, "type")
        lm = 0
        if kind == "edge":
            if labels:
                raise ValueError("an edge has a type and no labels")
        else:
            lreg = self.reg("vertex", "label")
            for l in labels:
                lm |= lreg.resolve(l)
        tm = treg.resolve(type_name)
        eid = norm_id(id_) if id_ is not None else new_id()
        if id_ is not None and eid in (self._vidx if kind == "vertex" else self._eidx):
            raise ValueError(f"duplicate {kind} id {eid}")
        return lm, tm, eid

    def commit(self, g: Graph, kind: str, el: Any, lm: int, tm: int, eid: str) -> None:
        pms = g.vp if kind == "vertex" else g.ep
        if kind == "vertex":
            pms[self.label_map][el] = self.to_stored(lm)
        pms[self.type_map][el] = self.to_stored(tm)
        pms[self.id_map][el] = eid
        self._index_put(kind, eid, el)
        self.computed.clear()                # topology changed: scores are stale

    def retire(self, g: Graph, kind: str, el: Any) -> None:
        """Forget an element's id and zero its masks (so scans never see it)."""
        pms = g.vp if kind == "vertex" else g.ep
        eid = pms[self.id_map][el]
        if eid:
            (self._vidx if kind == "vertex" else self._eidx).pop(eid, None)
        if kind == "vertex":
            pms[self.label_map][el] = 0
        pms[self.type_map][el] = 0
        pms[self.id_map][el] = ""
        self.computed.clear()


def _link() -> None:
    """Bind the names this module uses from modules loaded after it (they import this one).
    Called once by the package __init__, after every module is loaded."""
    global PropertyStore, _REV_MARKER, _StoreIdIndex, _is_e, _is_v, _norm_or_raw
    from .elements import _is_e, _is_v
    from .stores import PropertyStore
    from .cassandra_store import _REV_MARKER, _StoreIdIndex
    from .engine import _norm_or_raw
