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

"""openCypher: MATCH machinery (pattern expansion)."""
from __future__ import annotations

from graph_tool import Vertex
from .errors import OpenCypherError
from .common import CORE_KEYS, _CHUNKS, _norm
from .ast_nodes import PNode, PPart, PRel
from .values import CPath, _ekey, _eq
from .elements import _SemiJoin, _alive, _prefetch_objs, _prop
from .execution import _HID, _Run as _RunBase, _and_terms, _is_const
from .graph_functions import _incident

# =========================================================================== #
# openCypher: MATCH machinery
# =========================================================================== #
def _flip(d: str) -> str:
    return {"out": "in", "in": "out", "both": "both"}[d]


class _Run(_RunBase):                                 # the runner, continued: matching
    # -- hints from WHERE ---------------------------------------------------- #
    def where_hints(self, where) -> dict:
        """var -> {"labels": set, "eq": {key: [values]}} from top-level AND terms of a WHERE.
        Used only to narrow candidates early; the WHERE is still evaluated in full."""
        hints: dict = {}
        for t in _and_terms(where):
            if t[0] == "cmp" and t[1] == "=":
                for a, b in ((t[2], t[3]), (t[3], t[2])):
                    if a[0] == "prop" and a[1][0] == "var" and _is_const(b):
                        v = self.ev(b, {})
                        if v is not None:
                            hints.setdefault(a[1][1], {"labels": set(), "eq": {}})["eq"].setdefault(a[2], []).append(v)
                        break
            elif t[0] == "in" and t[1][0] == "prop" and t[1][1][0] == "var" and _is_const(t[2]):
                v = self.ev(t[2], {})
                if isinstance(v, list) and v and all(x is not None for x in v):
                    hints.setdefault(t[1][1][1], {"labels": set(), "eq": {}})["eq"].setdefault(t[1][2], []).extend(v)
            elif t[0] == "haslabel" and t[1][0] == "var":
                hints.setdefault(t[1][1], {"labels": set(), "eq": {}})["labels"].update(t[2])
        return hints

    # -- node constraints ---------------------------------------------------- #
    def node_cons(self, node: PNode, row: dict, hint: dict | None):
        """-> (labels tuple, eqs {key: [allowed values]}) for a pattern node in `row`."""
        labels = set(node.labels)
        eqs: dict = {}
        props = node.props
        if props:
            for k, ex in props:
                v = self.ev(ex, row)
                eqs.setdefault(k, []).append(v)
        if hint:
            labels |= hint["labels"]
            for k, vs in hint["eq"].items():
                eqs.setdefault(k, []).append(vs)         # a hint is "value in vs"
        return tuple(sorted(labels)), eqs

    @staticmethod
    def _allowed(spec) -> list:
        return spec if isinstance(spec, list) else [spec]

    def _val_ok(self, actual, specs: list) -> bool:
        """actual must satisfy EVERY spec: an inline value (equal) or a hint list (member)."""
        for s in specs:
            if isinstance(s, list):                       # hint: value IN list
                if not any(_eq(actual, x) is True for x in s):
                    return False
            elif _eq(actual, s) is not True:
                return False
        return True

    def filter_nodes(self, cands: list, labels: tuple, eqs: dict) -> list:
        """Keep vertices having all `labels` and satisfying every equality, batching store reads."""
        g, sc, store = self.g, self.sc, self.store
        if labels:
            cands = [v for v in cands if sc.has_label(g, v, labels, "all")]
        if not eqs or not cands:
            return cands
        if any(any(s is None for s in specs if not isinstance(s, list)) for specs in eqs.values()):
            return []                                     # {k: null} never matches
        # semi-join on a store column once the batch is big
        if store is not None and len(cands) >= store.semijoin_min:
            for k, specs in eqs.items():
                if _norm(k) in CORE_KEYS or k in sc.computed:
                    continue
                vals = specs[0] if isinstance(specs[0], list) else [specs[0]]
                sj = _SemiJoin(store, k, vals)
                verdicts = [sj.verdict(g, sc, v) for v in cands]
                if any(x is not None for x in verdicts):
                    cands = [v for v, x in zip(cands, verdicts) if x is not False]
                    break
        if store is not None:
            _prefetch_objs(g, cands)
        return [v for v in cands if all(self._val_ok(_prop(g, v, k), specs) for k, specs in eqs.items())]

    # -- seeds ----------------------------------------------------------------- #
    def seed_how(self, labels: tuple, eqs: dict) -> tuple:
        """How candidates for an unbound node are found: ("id"|"store"|"mask"|"scan", detail)."""
        if "id" in eqs:
            return ("id", "id")
        if self.store is not None:
            for k in eqs:
                if _norm(k) not in CORE_KEYS and k not in self.sc.computed and self._searchable(k):
                    return ("store", k)
        if labels:
            return ("mask", labels)
        return ("scan", None)

    def _searchable(self, key: str) -> bool:
        # a probe is expensive and exact searchability is per-table; try the store search lazily
        return True

    def seed(self, labels: tuple, eqs: dict) -> list:
        g, sc, store = self.g, self.sc, self.store
        how, detail = self.seed_how(labels, eqs)
        alive = None
        if how == "id":
            vals = [x for s in eqs["id"] for x in (s if isinstance(s, list) else [s])]
            sc.prefetch_ids("vertex", vals)
            out = []
            for x in vals:
                try:
                    v = sc.vertex_by_id(g, x)
                except ValueError:
                    continue
                if v is not None and _alive(g, v):
                    out.append(v)
            return out
        if how == "store":
            specs = eqs[detail]
            vals = specs[0] if isinstance(specs[0], list) else [specs[0]]
            if any(v is None for v in vals):
                return []
            if store.has_pending():
                store.flush()                             # searches only see flushed rows
            types = set(sc.reg("vertex", "type").all_names())
            rows = store.find_rows("vertex", detail, list(vals), types)
            if rows is not None:
                vs = []
                sc.prefetch_ids("vertex", [i for _, i, _ in rows])
                for tn, id_, _row in rows:
                    try:
                        v = sc.vertex_by_id(g, id_)
                    except ValueError:
                        continue
                    if v is not None and _alive(g, v) and sc.type_name(g, v) == tn:
                        vs.append(v)
                vs.sort(key=int)
                self.stats["seed_store"] += 1
                return vs
            how = "mask" if labels else "scan"            # column not searchable: scan
        if how == "mask" or labels:
            if not labels:
                return list(self._all_alive())
            idx = sc.candidates(g, "vertex", [("label", labels, "all")])
            self.stats["seed_mask"] += 1
            return [v for v in (g.vertex(int(i)) for i in idx) if _alive(g, v)]
        self.stats["seed_scan"] += 1
        return list(self._all_alive())

    def _all_alive(self):
        if "_dropped" in g_vp(self.g):
            return (v for v in self.g.vertices() if _alive(self.g, v))
        return self.g.vertices()

    # -- plan: which node of the chain to start from ----------------------------- #
    def choose_start(self, part: PPart, bound: set, hints: dict) -> int:
        names = [n.var for n in part.nodes]
        for i, nm in enumerate(names):
            if nm is not None and nm in bound:
                return i
        if len(part.nodes) == 1:
            return 0
        best, best_i = None, 0
        for i, n in enumerate(part.nodes):
            h = hints.get(n.var) if n.var else None
            has_eq = bool(n.props) or bool(h and h["eq"])
            labels = tuple(sorted(set(n.labels) | (h["labels"] if h else set())))
            if has_eq and self.store is not None:
                score = (0, 0)
            elif labels:
                est = len(self.sc.candidates(self.g, "vertex", [("label", labels, "all")]))
                score = (1 if has_eq else 2, est)
            else:
                score = (3, self.g.num_vertices())
            if best is None or score < best:
                best, best_i = score, i
        return best_i

    # -- the matcher ---------------------------------------------------------------- #
    def match_rows(self, rows: list, part: PPart, hints: dict | None = None):
        """Generator of row-chunks: all extensions of `rows` that match one pattern part."""
        hints = hints or {}
        if part.mode:
            yield from self.match_shortest(rows, part, hints)
            return
        names = [n.var if n.var is not None else f"{_HID}n{i}" for i, n in enumerate(part.nodes)]
        rnames = [r.var if r.var is not None else f"{_HID}r{i}" for i, r in enumerate(part.rels)]
        bound = set(rows[0]) if rows else set()
        s = self.choose_start(part, bound, hints)
        steps = [(i, i, i + 1, True) for i in range(s, len(part.rels))] + \
                [(i, i + 1, i, False) for i in range(s - 1, -1, -1)]
        for sub in self.start_chunks(rows, part, names, s, hints):
            cur = sub
            for (ri, a, b, fwd) in steps:
                if not cur:
                    break
                cur = self.expand(cur, part, names, rnames, ri, a, b, fwd, hints)
            if not cur:
                continue
            out = []
            for r in cur:
                if part.path_var:
                    r[part.path_var] = self.build_path(r, part, names, rnames)
                for k in [k for k in r if k.startswith(_HID) and k[1:2] in ("n", "r", "v")]:
                    del r[k]
                out.append(r)
            yield out

    def start_chunks(self, rows, part, names, s, hints):
        """Bind the start node: reuse a bound variable, or enumerate seed candidates in growing
        sub-chunks (lazy, so LIMIT can stop early)."""
        node, nm = part.nodes[s], names[s]
        h = hints.get(node.var) if node.var else None
        if rows and nm in rows[0]:                        # already bound
            out = []
            for r in rows:
                v = r.get(nm)
                if isinstance(v, Vertex) and self.filter_nodes([v], *self.node_cons(node, r, h)):
                    out.append(dict(r))
            if out:
                yield out
            return
        const = not node.props or _is_const([ex for _, ex in node.props])
        shared = None
        for r in rows:
            labels, eqs = self.node_cons(node, r, h)
            if const and shared is not None:
                cands = shared
            else:
                cands = self.seed(labels, eqs)
                if const:
                    shared = cands
            i, pos = 0, 0
            while pos < len(cands):
                take = cands[pos:pos + _CHUNKS[min(i, len(_CHUNKS) - 1)]]
                pos += len(take)
                i += 1
                ok = self.filter_nodes(take, labels, eqs)
                if ok:
                    yield [dict(r, **{nm: v}) for v in ok]

    def build_path(self, r, part, names, rnames) -> CPath:
        nodes, rels = [r[names[0]]], []
        for i, rel in enumerate(part.rels):
            if rel.varlen:
                nodes.extend(r.get(f"{_HID}vn{i}", []))
                rels.extend(r[rnames[i]])
            else:
                rels.append(r[rnames[i]])
            nodes.append(r[names[i + 1]])
        return CPath(tuple(nodes), tuple(rels))

    def other_end(self, v, e):
        return e.target() if e.source() == v else e.source()

    def rel_type_ok(self, e, rel: PRel) -> bool:
        return (not rel.types) or self.sc.has_type(self.g, e, rel.types, "any")

    def expand(self, rows, part, names, rnames, ri, a, b, fwd, hints):
        rel = part.rels[ri]
        direction = rel.direction if fwd else _flip(rel.direction)
        d = {"out": "out", "in": "in", "both": "all"}[direction]
        to_node = part.nodes[b]
        h = hints.get(to_node.var) if to_node.var else None
        to_name = names[b]
        g = self.g
        if rel.varlen:
            return self.expand_varlen(rows, part, names, rnames, ri, a, b, fwd, direction, h)
        trips = []                                         # (row, edge, other)
        for r in rows:
            self.tick()
            v = r[names[a]]
            if not isinstance(v, Vertex):
                continue
            used = r[_HID + "E"]
            tgt = r.get(to_name)
            for e in _incident(g, v, d):
                if not self.rel_type_ok(e, rel):
                    continue
                if _ekey(g, e) in used:
                    continue
                o = self.other_end(v, e)
                if tgt is not None and to_name in r and int(o) != int(tgt):
                    continue
                trips.append((r, e, o))
        if rel.props and trips:
            if self.store is not None:
                _prefetch_objs(g, [e for _, e, _ in trips])
            keep = []
            for r, e, o in trips:
                if all(self._val_ok(_prop(g, e, k), [self.ev(ex, r)]) for k, ex in rel.props):
                    keep.append((r, e, o))
            trips = keep
        # node checks only for NEW bindings
        todo = [(r, e, o) for (r, e, o) in trips if to_name not in r]
        done = [(r, e, o) for (r, e, o) in trips if to_name in r]
        const = not to_node.props or _is_const([ex for _, ex in to_node.props])
        if todo:
            if const:
                labels, eqs = self.node_cons(to_node, todo[0][0], h)
                uniq = {int(o): o for _, _, o in todo}
                okset = {int(v) for v in self.filter_nodes(list(uniq.values()), labels, eqs)}
                todo = [(r, e, o) for (r, e, o) in todo if int(o) in okset]
            else:
                if self.store is not None:
                    _prefetch_objs(g, [o for _, _, o in todo])
                todo = [(r, e, o) for (r, e, o) in todo
                        if self.filter_nodes([o], *self.node_cons(to_node, r, h))]
        else:
            pass
        # a bound target still has to satisfy its own constraints in this pattern
        if done:
            keep = []
            for r, e, o in done:
                if self.filter_nodes([o], *self.node_cons(to_node, r, h)):
                    keep.append((r, e, o))
            done = keep
        out = []
        for r, e, o in done + todo:
            nr = dict(r)
            nr[to_name] = o
            nr[rnames[ri]] = e
            nr[_HID + "E"] = r[_HID + "E"] + (_ekey(g, e),)
            out.append(nr)
        return out

    def expand_varlen(self, rows, part, names, rnames, ri, a, b, fwd, direction, h):
        rel, g = part.rels[ri], self.g
        d = {"out": "out", "in": "in", "both": "all"}[direction]
        to_node, to_name = part.nodes[b], names[b]
        cap = rel.hi if rel.hi is not None else self.eng.varlen_cap
        out = []
        for r in rows:
            v0 = r[names[a]]
            if not isinstance(v0, Vertex):
                continue
            used0 = set(r[_HID + "E"])
            tgt = r.get(to_name)
            # level-synchronous trail enumeration: state = (vertex, edges, vertices-between)
            level = [(v0, (), ())]
            results = []
            if rel.lo == 0:
                results.append((v0, (), ()))
            depth = 0
            while level and depth < cap:
                depth += 1
                nxt = []
                cand = []
                for v, es, vs in level:
                    self.tick()
                    seen_here = {_ekey(g, e) for e in es}
                    for e in _incident(g, v, d):
                        k = _ekey(g, e)
                        if k in used0 or k in seen_here or not self.rel_type_ok(e, rel):
                            continue
                        cand.append((v, es, vs, e))
                if rel.props and cand and self.store is not None:
                    _prefetch_objs(g, [c[3] for c in cand])
                for v, es, vs, e in cand:
                    if rel.props and not all(self._val_ok(_prop(g, e, k), [self.ev(ex, r)]) for k, ex in rel.props):
                        continue
                    o = self.other_end(v, e)
                    nes, nvs = es + (e,), (vs + (v,) if es else vs)
                    nxt.append((o, nes, nvs))
                    if depth >= rel.lo:
                        results.append((o, nes, nvs))
                level = nxt
            if tgt is not None and to_name in r:
                results = [x for x in results if int(x[0]) == int(tgt)]
            ends = {int(o): o for o, _, _ in results}
            labels, eqs = self.node_cons(to_node, r, h)
            ok = {int(v) for v in self.filter_nodes(list(ends.values()), labels, eqs)}
            for o, es, vs in results:
                if int(o) not in ok:
                    continue
                es_l, vs_l = list(es), list(vs)
                if not fwd:                                # built right-to-left: restore pattern order
                    es_l.reverse(); vs_l.reverse()
                nr = dict(r)
                nr[to_name] = o
                nr[rnames[ri]] = es_l
                nr[f"{_HID}vn{ri}"] = vs_l
                nr[_HID + "E"] = r[_HID + "E"] + tuple(_ekey(g, e) for e in es)
                out.append(nr)
        return out

    # -- shortestPath() / allShortestPaths() ----------------------------------------- #
    def match_shortest(self, rows, part: PPart, hints: dict):
        if len(part.rels) != 1 or not part.rels[0].varlen:
            raise OpenCypherError("shortestPath() needs a single variable-length relationship pattern")
        rel, g = part.rels[0], self.g
        names = [n.var if n.var is not None else f"{_HID}n{i}" for i, n in enumerate(part.nodes)]
        d = {"out": "out", "in": "in", "both": "all"}[rel.direction]
        out = []
        for r in rows:
            ends = []
            for i in (0, 1):
                node = part.nodes[i]
                h = hints.get(node.var) if node.var else None
                labels, eqs = self.node_cons(node, r, h)
                if names[i] in r:
                    v = r[names[i]]
                    ends.append(self.filter_nodes([v], labels, eqs) if isinstance(v, Vertex) else [])
                else:
                    ends.append(self.filter_nodes(self.seed(labels, eqs), labels, eqs))
            cap = rel.hi if rel.hi is not None else self.eng.varlen_cap
            for a in ends[0]:
                for b in ends[1]:
                    if int(a) == int(b) and rel.lo > 0:
                        continue
                    for es, vs in self.bfs_paths(a, b, rel, d, cap, part.mode == "all"):
                        nr = dict(r)
                        nr[names[0]], nr[names[1]] = a, b
                        if rel.var:
                            nr[rel.var] = list(es)
                        if part.path_var:
                            nr[part.path_var] = CPath(tuple(vs), tuple(es))
                        for k in [k for k in nr if k.startswith(_HID) and k[1:2] in ("n", "r", "v")]:
                            del nr[k]
                        out.append(nr)
        if out:
            yield out

    def bfs_paths(self, a, b, rel: PRel, d: str, cap: int, all_paths: bool):
        """Shortest path(s) a->b: BFS by layers keeping every predecessor at minimal depth."""
        g = self.g
        if int(a) == int(b):
            return [((), (a,))]
        dist = {int(a): 0}
        preds: dict[int, list] = {}
        frontier = [a]
        depth = 0
        found = False
        while frontier and depth < cap and not found:
            depth += 1
            nxt = {}
            for v in frontier:
                self.tick()
                for e in _incident(g, v, d):
                    if not self.rel_type_ok(e, rel):
                        continue
                    o = self.other_end(v, e)
                    oi = int(o)
                    if oi in dist and dist[oi] < depth:
                        continue
                    if oi not in dist:
                        dist[oi] = depth
                        nxt[oi] = o
                    preds.setdefault(oi, []).append((v, e))
                    if oi == int(b):
                        found = True
            frontier = list(nxt.values())
        if not found or depth < rel.lo:
            return []
        results = []

        def walk(v, es, vs):
            if int(v) == int(a):
                results.append((tuple(reversed(es)), tuple(reversed(vs))))
                return
            for (p, e) in preds.get(int(v), []):
                if all_paths or not results:
                    walk(p, es + [e], vs + [p])
        walk(b, [], [b])
        return results if all_paths else results[:1]


def g_vp(g):
    return g.vp
