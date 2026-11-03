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

"""openCypher: clauses (CREATE, MERGE, SET, DELETE, WITH, RETURN, ...)."""
from __future__ import annotations

from graph_tool import Edge
from graph_tool import Graph
from graph_tool import Vertex
from typing import Any
from typing import Iterator
from .errors import OpenCypherError
from .locking import _check_write
from .common import _txn_of
from .ids import stable_id
from .ast_nodes import Call, Create, Delete, Match, Merge, PPart, Proj, Remove, SetC, Single, Unwind, _has_agg
from .values import CPath, _ekey, _hk, _rank
from .schema import Schema
from .elements import _all_keys, _create_edge, _create_vertex, _prefetch_objs, _set_props, _undo_edge_delete, _undo_vertex_delete
from .execution import _HID
from .matching import _Run as _RunBase

# =========================================================================== #
# openCypher: clauses
# =========================================================================== #
def _flatten(chunks) -> list:
    return [r for c in chunks for r in c]


def _clean(r: dict) -> dict:
    r.pop(_HID + "E", None)
    r.pop(_HID + "i", None)
    return r


def _pattern_vars(parts) -> list:
    out = []
    for p in parts:
        if p.path_var: out.append(p.path_var)
        for n in p.nodes:
            if n.var: out.append(n.var)
        for r in p.rels:
            if r.var: out.append(r.var)
    return out


def _popcount(x: int) -> int:
    return bin(x & ((1 << 64) - 1)).count("1")


def _msg(ex: Exception) -> str:
    return str(ex.args[0]) if isinstance(ex, KeyError) and ex.args else str(ex)


class _Run(_RunBase):                                 # the runner, continued: clauses
    def match_parts(self, rows, parts, hints):
        if not parts:
            yield rows
            return
        for sub in self.match_rows(rows, parts[0], hints):
            yield from self.match_parts(sub, parts[1:], hints)

    def clause_match(self, chunks, c: Match):
        hints = {}
        newvars = _pattern_vars(c.parts)
        for chunk in chunks:
            if not hints and c.where is not None:
                hints = self.where_hints(c.where)
            rows = []
            for i, r in enumerate(chunk):
                nr = dict(r)
                nr[_HID + "E"] = ()
                if c.optional:
                    nr[_HID + "i"] = i
                rows.append(nr)
            matched: set = set()
            for sub in self.match_parts(rows, c.parts, hints):
                if c.where is not None:
                    self.prefetch(sub, [c.where])
                    sub = [r for r in sub if self.ev(c.where, r) is True]
                if not sub:
                    continue
                if c.optional:
                    matched.update(r[_HID + "i"] for r in sub)
                yield [_clean(r) for r in sub]
            if c.optional:
                miss = [chunk[i] for i in range(len(chunk)) if i not in matched]
                if miss:
                    yield [dict({v: None for v in newvars if v not in r}, **r) for r in miss]

    def clause_unwind(self, chunks, c: Unwind):
        for chunk in chunks:
            out = []
            for r in chunk:
                v = self.ev(c.expr, r)
                if v is None:
                    continue
                for x in (v if isinstance(v, (list, tuple)) else [v]):
                    nr = dict(r); nr[c.var] = x
                    out.append(nr)
            if out:
                yield out

    # -- WITH / RETURN ---------------------------------------------------------- #
    def _count_arg(self, e, what):
        if e is None:
            return None
        v = self.ev(e, {})
        if not isinstance(v, int) or isinstance(v, bool) or v < 0:
            raise OpenCypherError(f"{what} must be a non-negative integer")
        return v

    def clause_proj(self, chunks, p: Proj):
        items = p.items
        agg = any(_has_agg(i.expr) for i in items)
        skip, limit = self._count_arg(p.skip, "SKIP"), self._count_arg(p.limit, "LIMIT")
        exprs = [i.expr for i in items] + [o[0] for o in p.order] + [p.where]
        if not agg and not p.order:
            yield from self._proj_stream(chunks, p, skip, limit, exprs)
            return
        rows = _flatten(chunks)
        self.prefetch(rows, exprs)
        outs = []                                          # (projected row, env row, group rows)
        if agg:
            keys_items = [i for i in items if not _has_agg(i.expr)]
            groups: dict = {}
            order_keys: list = []
            for r in rows:
                k = tuple(_hk(self.ev(i.expr, r)) for i in keys_items)
                if k not in groups:
                    groups[k] = (r, [])
                    order_keys.append(k)
                groups[k][1].append(r)
            if not groups and not keys_items:
                groups[()] = ({}, [])
                order_keys.append(())
            for k in order_keys:
                first, grp = groups[k]
                nr = self._star(first, p) if p.star else {}
                for i in items:
                    nr[i.alias] = self.ev(i.expr, first, grp)
                outs.append((nr, dict(first, **nr), grp))
        else:
            for r in rows:
                nr = self._star(r, p) if p.star else {}
                for i in items:
                    nr[i.alias] = self.ev(i.expr, r)
                outs.append((nr, dict(r, **nr), None))
        if p.distinct:
            seen, keep = set(), []
            for o in outs:
                k = tuple((kk, _hk(vv)) for kk, vv in o[0].items())
                if k not in seen:
                    seen.add(k)
                    keep.append(o)
            outs = keep
        if p.order:
            self.prefetch([o[1] for o in outs], [x[0] for x in p.order])
            for expr, desc in reversed(p.order):
                outs.sort(key=lambda o, e=expr: _rank(self.ev(e, o[1], o[2])), reverse=desc)
        res = [o[0] for o in outs]
        if skip:
            res = res[skip:]
        if limit is not None:
            res = res[:limit]
        if p.where is not None:
            res = [r for r in res if self.ev(p.where, r) is True]
        for i in range(0, len(res), 512):
            yield res[i:i + 512]

    def _star(self, r: dict, p: Proj) -> dict:
        return {k: v for k, v in r.items() if not k.startswith(_HID)} if p.star else {}

    def _proj_stream(self, chunks, p: Proj, skip, limit, exprs):
        seen: set = set()
        to_skip, left = skip or 0, limit
        if left == 0:
            return
        for chunk in chunks:
            self.prefetch(chunk, exprs)
            out = []
            for r in chunk:
                nr = self._star(r, p)
                for i in p.items:
                    nr[i.alias] = self.ev(i.expr, r)
                if p.distinct:
                    k = tuple((kk, _hk(vv)) for kk, vv in nr.items())
                    if k in seen:
                        continue
                    seen.add(k)
                if to_skip:
                    to_skip -= 1
                    continue
                if left is not None:
                    if left <= 0:
                        break
                    left -= 1
                out.append(nr)
            if p.where is not None:
                out = [r for r in out if self.ev(p.where, r) is True]
            if out:
                yield out
            if left is not None and left <= 0:
                return

    # -- writes ---------------------------------------------------------------------- #
    def _vals(self, props, row) -> dict:
        d = {}
        for k, ex in (props or []):
            d[k] = self.ev(ex, row)
        return d

    def create_part(self, row: dict, part: PPart, merge: bool = False) -> None:
        g = self.g
        nodes = []
        for node in part.nodes:
            cur = row.get(node.var) if node.var else None
            if node.var and node.var in row and cur is not None:
                nodes.append(cur)
                continue
            props = self._vals(node.props, row)
            id_ = props.pop("id", None)
            if id_ is None and merge:
                id_ = _merge_id(self.eng, "vertex", props, *sorted(node.labels))
            try:
                v = _create_vertex(g, tuple(node.labels), None, id_)
            except (KeyError, ValueError) as ex:
                raise OpenCypherError(_msg(ex)) from None
            self.stats["nodes_created"] += 1
            self.stats["labels_added"] += len(node.labels)
            props = {k: x for k, x in props.items() if x is not None}
            if props:
                self.set_props(v, props)
            if node.var:
                row[node.var] = v
            nodes.append(v)
        rels = []
        for i, rel in enumerate(part.rels):
            if len(rel.types) != 1:
                raise OpenCypherError("CREATE/MERGE needs exactly one relationship type")
            if rel.varlen:
                raise OpenCypherError("variable-length relationships cannot be created")
            if rel.direction == "both" and not merge:
                raise OpenCypherError("CREATE requires a directed relationship")
            a, b = nodes[i], nodes[i + 1]
            if rel.direction == "in":
                a, b = b, a
            props = self._vals(rel.props, row)
            id_ = props.pop("id", None)
            if id_ is None and merge:
                id_ = _merge_id(self.eng, "edge", props, rel.types[0],
                                self.sc.get_id(g, a), self.sc.get_id(g, b))
            try:
                e = _create_edge(g, a, b, rel.types[0], None, id_)
            except (KeyError, ValueError) as ex:
                raise OpenCypherError(_msg(ex)) from None
            self.stats["relationships_created"] += 1
            props = {k: x for k, x in props.items() if x is not None}
            if props:
                self.set_props(e, props)
            if rel.var:
                row[rel.var] = e
            rels.append(e)
        if part.path_var:
            row[part.path_var] = CPath(tuple(nodes), tuple(rels))

    def clause_create(self, chunks, c: Create):
        rows = _flatten(chunks)
        for r in rows:
            for part in c.parts:
                self.create_part(r, part)
        for i in range(0, len(rows), 512):
            yield rows[i:i + 512]

    def clause_merge(self, chunks, c: Merge):
        rows = _flatten(chunks)
        out = []
        for r in rows:
            r0 = dict(r); r0[_HID + "E"] = ()
            found = _flatten(self.match_rows([r0], c.part, {}))
            if found:
                found = [_clean(m) for m in found]
                if c.on_match:
                    self.apply_sets(found, c.on_match)
                out.extend(found)
            else:
                nr = dict(r)
                self.create_part(nr, c.part, merge=True)
                if c.on_create:
                    self.apply_sets([nr], c.on_create)
                out.append(nr)
        for i in range(0, len(out), 512):
            yield out[i:i + 512]

    def set_props(self, el, props: dict) -> None:
        if el is None:
            return
        if not isinstance(el, (Vertex, Edge)):
            raise OpenCypherError(f"SET needs a node or relationship, got {type(el).__name__}")
        try:
            _set_props(self.g, el, props)
        except (KeyError, ValueError) as ex:
            raise OpenCypherError(_msg(ex)) from None
        self.stats["properties_set"] += len(props)

    def label_change(self, v, labels, add: bool) -> None:
        if v is None:
            return
        if not isinstance(v, Vertex):
            raise OpenCypherError("labels can only be changed on nodes")
        g, sc = self.g, self.sc
        _check_write(g)
        reg = sc.reg("vertex", "label")
        m = 0
        for l in labels:
            m |= reg.resolve(l) if add else reg.lookup(l)
        cur = sc.label_mask(g, v)
        new = (cur | m) if add else (cur & ~m)
        if new != cur:
            t = _txn_of(g)
            if t is not None:
                pm = g.vp[sc.label_map]
                t.undo.append(lambda pm=pm, v=v, old=pm[v]: pm.__setitem__(v, old))
                t.redo.append(["vl", sc.get_id(g, v), reg.names(new)])
            g.vp[sc.label_map][v] = sc.to_stored(new)
            self.stats["labels_added" if add else "labels_removed"] += _popcount(new ^ cur)

    def apply_sets(self, rows, items) -> None:
        for r in rows:
            for it in items:
                kind = it[0]
                if kind == "prop":
                    _, target, ex = it
                    el = self.ev(target[1], r)
                    if el is None:
                        continue
                    self.set_props(el, {target[2]: self.ev(ex, r)})
                elif kind in ("merge", "replace"):
                    el = r.get(it[1])
                    m = self.ev(it[2], r)
                    if el is None:
                        continue
                    if isinstance(m, (Vertex, Edge)):
                        m = self.props_of(m)
                    if not isinstance(m, dict):
                        raise OpenCypherError(f"SET {it[1]} {'+=' if kind == 'merge' else '='} needs a map")
                    if kind == "replace":
                        for k in _all_keys(self.g, el):
                            m.setdefault(k, None)
                    self.set_props(el, m)
                else:
                    self.label_change(r.get(it[1]), it[2], True)

    def clause_set(self, chunks, c: SetC):
        rows = _flatten(chunks)
        self.apply_sets(rows, c.items)
        for i in range(0, len(rows), 512):
            yield rows[i:i + 512]

    def clause_remove(self, chunks, c: Remove):
        rows = _flatten(chunks)
        for r in rows:
            for it in c.items:
                if it[0] == "prop":
                    el = r.get(it[1])
                    if el is not None:
                        self.set_props(el, {it[2]: None})
                else:
                    self.label_change(r.get(it[1]), it[2], False)
        for i in range(0, len(rows), 512):
            yield rows[i:i + 512]

    def clause_delete(self, chunks, c: Delete):
        rows = _flatten(chunks)
        g = self.g
        verts: dict[int, Vertex] = {}
        edges: dict[int, Edge] = {}

        def collect(x):
            if x is None:
                return
            if isinstance(x, Vertex):
                verts[int(x)] = x
            elif isinstance(x, Edge):
                edges[_ekey(g, x)] = x
            elif isinstance(x, CPath):
                for n in x.nodes: collect(n)
                for e in x.rels: collect(e)
            elif isinstance(x, (list, tuple)):
                for i in x: collect(i)
            else:
                raise OpenCypherError(f"DELETE needs nodes, relationships or paths, got {type(x).__name__}")
        for r in rows:
            for ex in c.exprs:
                collect(self.ev(ex, r))
        if c.detach:
            for v in verts.values():
                for e in v.all_edges():
                    edges[_ekey(g, e)] = e
        else:
            for v in verts.values():
                if any(_ekey(g, e) not in edges for e in v.all_edges()):
                    raise OpenCypherError("Cannot delete node, because it still has relationships. "
                                      "To delete this node, you must first delete its relationships, "
                                      "or use DETACH DELETE")
        self.stats["relationships_deleted"] += len(edges)
        self.stats["nodes_deleted"] += len(verts)
        _delete_elements(g, self.sc, self.store, verts, edges)
        for i in range(0, len(rows), 512):
            yield rows[i:i + 512]

    # -- CALL ----------------------------------------------------------------------------- #
    def clause_call(self, chunks, c: Call):
        proc = _PROCS.get(c.name.lower())
        if proc is None:
            raise OpenCypherError(f"There is no procedure with the name `{c.name}` registered")
        for chunk in chunks:
            out = []
            for r in chunk:
                args = [self.ev(a, r) for a in c.args]
                fields, prows = proc(self, *args)
                ys = c.yields or [(f, f) for f in fields]
                for f, _ in ys:
                    if f not in fields:
                        raise OpenCypherError(f"procedure {c.name} has no output field `{f}`")
                for pr in prows:
                    nr = dict(r)
                    for f, alias in ys:
                        nr[alias] = pr[f]
                    if c.where is None or self.ev(c.where, nr) is True:
                        out.append(nr)
            if out:
                yield out

    # -- one query part ----------------------------------------------------------------------- #
    def run_single(self, q: Single, columns_only: bool = False):
        chunks: Iterator = iter([[{}]])
        scope: list = []
        cols: list | None = None
        returned = False
        for c in q.clauses:
            if isinstance(c, Match):
                chunks = self.clause_match(chunks, c)
                scope += [v for v in _pattern_vars(c.parts) if v not in scope]
            elif isinstance(c, Proj):
                chunks = self.clause_proj(chunks, c)
                names = ([s for s in scope] if c.star else []) + [i.alias for i in c.items]
                scope = list(dict.fromkeys(names))
                if c.kind == "return":
                    cols, returned = scope, True
            elif isinstance(c, Unwind):
                chunks = self.clause_unwind(chunks, c)
                scope.append(c.var)
            elif isinstance(c, Create):
                chunks = self.clause_create(chunks, c)
                scope += [v for v in _pattern_vars(c.parts) if v not in scope]
            elif isinstance(c, Merge):
                chunks = self.clause_merge(chunks, c)
                scope += [v for v in _pattern_vars([c.part]) if v not in scope]
            elif isinstance(c, SetC):
                chunks = self.clause_set(chunks, c)
            elif isinstance(c, Remove):
                chunks = self.clause_remove(chunks, c)
            elif isinstance(c, Delete):
                chunks = self.clause_delete(chunks, c)
            elif isinstance(c, Call):
                chunks = self.clause_call(chunks, c)
                scope += [a for _, a in (c.yields or [])] if c.yields else \
                    [f for f in _PROC_FIELDS.get(c.name.lower(), ())]
        if not returned and len(q.clauses) == 1 and isinstance(q.clauses[0], Call):
            cols, returned = list(scope), True            # standalone CALL returns its fields
        rows = _flatten(chunks)
        if not returned:
            return [], []
        return cols, rows

    def finish(self, cols: list, rows: list) -> list:
        """Internal rows -> returned rows (Node/Rel/Path objects), with batched store reads."""
        els = []
        def gather(x):
            if isinstance(x, (Vertex, Edge)): els.append(x)
            elif isinstance(x, CPath): els.extend(x.nodes); els.extend(x.rels)
            elif isinstance(x, (list, tuple)):
                for i in x: gather(i)
            elif isinstance(x, dict):
                for i in x.values(): gather(i)
        for r in rows:
            for c in cols:
                gather(r.get(c))
        if self.store is not None and els:
            _prefetch_objs(self.g, els)
        cache: dict = {}
        return [{c: self.out(r.get(c), cache) for c in cols} for r in rows]


def _delete_elements(g: Graph, sc: Schema, store, verts: dict, edges: dict) -> None:
    """Edges are removed (their rows deleted); vertices are TOMBSTONED, never removed,
    because graph-tool renumbers vertices on removal and that would orphan stored ids."""
    _check_write(g)
    t = _txn_of(g)
    for e in edges.values():
        tn, eid = sc.type_name(g, e), sc.get_id(g, e)
        if store is not None and tn and eid:
            store.stage_delete("edge", tn, eid)
        if t is not None and eid:
            t.undo.append(lambda args=(g, sc, e.source(), e.target(), sc.label_mask(g, e),
                                       sc.type_mask(g, e), eid): _undo_edge_delete(*args))
            t.redo.append(["e-", eid])
        sc.retire(g, "edge", e)
        g.remove_edge(e)
    if verts and "_dropped" not in g.vp:
        g.vp["_dropped"] = g.new_vertex_property("bool")
    for v in verts.values():
        tn, uid = sc.type_name(g, v), sc.get_id(g, v)
        if store is not None and tn and uid:
            store.stage_delete("vertex", tn, uid)
        if t is not None and uid:
            t.undo.append(lambda args=(g, sc, v, sc.label_mask(g, v), sc.type_mask(g, v), uid):
                          _undo_vertex_delete(*args))
            t.redo.append(["v-", uid])
        sc.retire(g, "vertex", v)
        g.vp["_dropped"][v] = True


def _merge_id(eng: "OpenCypher", kind: str, props: dict, *ident: Any) -> "str | None":
    """Deterministic id for an element created by MERGE (uuid5 of its identity), so a retried
    MERGE re-creates the SAME id and the store upsert is idempotent. None -> use a random id
    (option off, or that id is already taken, e.g. the node was later renamed)."""
    if not eng.stable_merge_ids:
        return None
    sig = repr(sorted((k, repr(v)) for k, v in props.items() if v is not None))
    sid = stable_id("opencypher-merge", kind, *ident, sig)
    idx = eng.schema._vidx if kind == "vertex" else eng.schema._eidx
    return None if sid in idx else sid


def _link() -> None:
    """Bind the names this module uses from modules loaded after it (they import this one).
    Called once by the package __init__, after every module is loaded."""
    global OpenCypher, _PROCS, _PROC_FIELDS
    from .procedures import _PROCS, _PROC_FIELDS
    from .engine import OpenCypher
