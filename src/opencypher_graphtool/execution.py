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

"""openCypher: execution (the query runner's core)."""
from __future__ import annotations

import re
import time
from collections import Counter
from graph_tool import Edge
from graph_tool import Vertex
from typing import Any
from .errors import OpenCypherError, QueryTimeout
from .common import CORE_KEYS
from .ast_nodes import _AGGS
from .values import CPath, _SCALAR_FUNCS, _agg_values, _and, _arith, _cmp, _eq, _i64, _is_num, _or, _truth
from .results import Node, Path, Rel
from .elements import _all_keys, _prefetch_objs, _prop

# =========================================================================== #
# openCypher: execution
# =========================================================================== #
_HID = "\0"                     # hidden row keys (never visible to queries)
_CONST_KINDS = {"lit", "param"}


def _is_const(e: Any) -> bool:
    """True when an expression references no variables (so it can be evaluated once)."""
    if isinstance(e, tuple):
        if e and e[0] in ("var", "pattern", "exists", "patcomp", "listcomp", "quant", "reduce"):
            return False
        return all(_is_const(x) for x in e[1:])
    if isinstance(e, list):
        return all(_is_const(x) for x in e)
    return True


def _needed_vars(e: Any, out: set) -> None:
    """Variables whose PROPERTIES (or full contents) an expression reads: used to batch-load store rows."""
    if isinstance(e, tuple) and e:
        if e[0] == "prop" and isinstance(e[1], tuple) and e[1][0] == "var":
            out.add(e[1][1])
        elif e[0] == "call" and e[1].lower() in ("properties", "keys") and e[2] \
                and isinstance(e[2][0], tuple) and e[2][0][0] == "var":
            out.add(e[2][0][1])
        for x in e[1:]:
            _needed_vars(x, out)
    elif isinstance(e, list):
        for x in e:
            _needed_vars(x, out)


def _and_terms(e: Any) -> list:
    return _and_terms(e[1]) + _and_terms(e[2]) if isinstance(e, tuple) and e[0] == "and" else ([e] if e else [])


_STATIC_ONLY = object()


class _Run:
    """One query execution: evaluator + clause implementations."""

    def __init__(self, eng: "OpenCypher", params: dict):
        self.eng, self.g, self.params = eng, eng.graph, params
        self.sc, self.store = eng.schema, eng.store
        self.stats: Counter = Counter()
        self._need_cache: dict[int, set] = {}
        self._plan_notes: list[str] = []
        self._ticks = 0
        self.vector_evals = 0                        # vector.* calls in this query (scan guard)
        self.deadline = None if eng.timeout is None else time.monotonic() + eng.timeout

    def tick(self) -> None:
        """Cooperative cancellation: hot loops call this; past cy.timeout it raises QueryTimeout
        (a write query is then rolled back like any other failure)."""
        self._ticks += 1
        if self._ticks >= 4096:
            self._ticks = 0
            if self.deadline is not None and time.monotonic() > self.deadline:
                raise QueryTimeout(f"query exceeded the {self.eng.timeout}s timeout")

    # ------------------------------------------------------------------ eval
    def ev(self, e, row, rows=None):
        self.tick()
        k = e[0]
        if k == "lit":
            return e[1]
        if k == "var":
            try:
                return row[e[1]]
            except KeyError:
                raise OpenCypherError(f"Variable `{e[1]}` not defined") from None
        if k == "param":
            if e[1] not in self.params:
                raise OpenCypherError(f"Expected parameter(s): {e[1]}")
            return self.params[e[1]]
        return getattr(self, "e_" + k)(e, row, rows)

    def e_prop(self, e, row, rows):
        return self.get_prop(self.ev(e[1], row, rows), e[2])

    def get_prop(self, base, key):
        if base is None:
            return None
        if isinstance(base, (Vertex, Edge)):
            return _prop(self.g, base, key)
        if isinstance(base, dict):
            return base.get(key)
        raise OpenCypherError(f"Type mismatch: expected a node, relationship or map, got {type(base).__name__}")

    def e_idx(self, e, row, rows):
        b, i = self.ev(e[1], row, rows), self.ev(e[2], row, rows)
        if b is None or i is None:
            return None
        if isinstance(b, dict):
            return b.get(i) if isinstance(i, str) else self._bad("map key must be a string")
        if isinstance(b, (list, tuple)):
            if not isinstance(i, int) or isinstance(i, bool):
                self._bad("list index must be an integer")
            return b[i] if -len(b) <= i < len(b) else None
        if isinstance(b, (Vertex, Edge)):
            return _prop(self.g, b, i)
        return self._bad(f"cannot index a {type(b).__name__}")

    def _bad(self, msg):
        raise OpenCypherError("Type mismatch: " + msg)

    def e_slice(self, e, row, rows):
        b = self.ev(e[1], row, rows)
        lo = self.ev(e[2], row, rows) if e[2] is not None else 0
        hi = self.ev(e[3], row, rows) if e[3] is not None else None
        if b is None or lo is None or (e[3] is not None and hi is None):
            return None
        return list(b[lo:hi])

    def e_list(self, e, row, rows):
        return [self.ev(x, row, rows) for x in e[1]]

    def e_map(self, e, row, rows):
        return {k: self.ev(v, row, rows) for k, v in e[1]}

    def e_neg(self, e, row, rows):
        v = self.ev(e[1], row, rows)
        if v is None:
            return None
        if not _is_num(v):
            self._bad("cannot negate a non-number")
        return _i64(-v)

    def e_bin(self, e, row, rows):
        return _arith(e[1], self.ev(e[2], row, rows), self.ev(e[3], row, rows))

    def e_cmp(self, e, row, rows):
        return _cmp(e[1], self.ev(e[2], row, rows), self.ev(e[3], row, rows))

    def e_and(self, e, row, rows):
        a = _truth(self.ev(e[1], row, rows))
        if a is False:
            return False
        return _and(a, _truth(self.ev(e[2], row, rows)))

    def e_or(self, e, row, rows):
        a = _truth(self.ev(e[1], row, rows))
        if a is True:
            return True
        return _or(a, _truth(self.ev(e[2], row, rows)))

    def e_xor(self, e, row, rows):
        a, b = _truth(self.ev(e[1], row, rows)), _truth(self.ev(e[2], row, rows))
        return None if a is None or b is None else a != b

    def e_not(self, e, row, rows):
        a = _truth(self.ev(e[1], row, rows))
        return None if a is None else not a

    def e_isnull(self, e, row, rows):
        v = self.ev(e[1], row, rows)
        return (v is not None) if e[2] else (v is None)

    def e_in(self, e, row, rows):
        a, lst = self.ev(e[1], row, rows), self.ev(e[2], row, rows)
        if lst is None:
            return None
        if not isinstance(lst, (list, tuple)):
            self._bad("IN needs a list")
        res = False
        for x in lst:
            r = _eq(a, x)
            if r is True:
                return True
            if r is None:
                res = None
        return res

    def e_strop(self, e, row, rows):
        a, b = self.ev(e[2], row, rows), self.ev(e[3], row, rows)
        if not (isinstance(a, str) and isinstance(b, str)):
            return None
        return a.startswith(b) if e[1] == "starts" else a.endswith(b) if e[1] == "ends" else (b in a)

    def e_regex(self, e, row, rows):
        a, b = self.ev(e[1], row, rows), self.ev(e[2], row, rows)
        if not (isinstance(a, str) and isinstance(b, str)):
            return None
        try:
            return re.fullmatch(b, a) is not None
        except re.error as ex:
            raise OpenCypherError(f"invalid regular expression: {ex}") from None

    def e_haslabel(self, e, row, rows):
        v = self.ev(e[1], row, rows)
        if v is None:
            return None
        if not isinstance(v, (Vertex, Edge)):
            self._bad("label predicate needs a node or relationship")
        return self.sc.has_label(self.g, v, e[2], "all")

    def e_case(self, e, row, rows):
        subject = self.ev(e[1], row, rows) if e[1] is not None else _STATIC_ONLY
        for w, t in e[2]:
            wv = self.ev(w, row, rows)
            if (_eq(subject, wv) is True) if subject is not _STATIC_ONLY else (wv is True):
                return self.ev(t, row, rows)
        return self.ev(e[3], row, rows) if e[3] is not None else None

    def e_listcomp(self, e, row, rows):
        _, var, src, where, proj = e
        lst = self.ev(src, row, rows)
        if lst is None:
            return None
        out = []
        for x in lst:
            r = dict(row); r[var] = x
            if where is not None and self.ev(where, r, rows) is not True:
                continue
            out.append(self.ev(proj, r, rows) if proj is not None else x)
        return out

    def e_quant(self, e, row, rows):
        _, kind, var, src, where = e
        lst = self.ev(src, row, rows)
        if lst is None:
            return None
        hits, nulls = 0, 0
        for x in lst:
            r = dict(row); r[var] = x
            v = self.ev(where, r, rows) if where is not None else True
            if v is True: hits += 1
            elif v is None: nulls += 1
        falses = len(lst) - hits - nulls
        if kind == "any":
            return True if hits else (None if nulls else False)
        if kind == "all":
            return False if falses else (None if nulls else True)
        if kind == "none":
            return False if hits else (None if nulls else True)
        if hits > 1:                                     # single
            return False
        return (None if nulls else True) if hits == 1 else (None if nulls else False)

    def e_reduce(self, e, row, rows):
        _, acc, init, var, src, body = e
        a = self.ev(init, row, rows)
        lst = self.ev(src, row, rows)
        if lst is None:
            return None
        for x in lst:
            r = dict(row); r[acc] = a; r[var] = x
            a = self.ev(body, r, rows)
        return a

    def e_pattern(self, e, row, rows):
        for _ in self.match_rows([dict(row, **{_HID + "E": ()})], e[1]):
            return True
        return False

    def e_exists(self, e, row, rows):
        for chunk in self.match_rows([dict(row, **{_HID + "E": ()})], e[1]):
            for r in chunk:
                if e[2] is None or self.ev(e[2], r) is True:
                    return True
        return False

    def e_patcomp(self, e, row, rows):
        out = []
        for chunk in self.match_rows([dict(row, **{_HID + "E": ()})], e[1]):
            for r in chunk:
                if e[2] is None or self.ev(e[2], r) is True:
                    out.append(self.ev(e[3], r))
        return out

    def e_star(self, e, row, rows):
        raise OpenCypherError("* is only valid inside count(*)")

    def e_call(self, e, row, rows):
        _, name, args, distinct = e
        low = name.lower()
        if low in _AGGS:
            if rows is None:
                raise OpenCypherError(f"Invalid use of aggregating function {name}(...) in this context")
            if args and args[0][0] == "star":
                return len(rows)
            vals = [self.ev(args[0], r) for r in rows]
            vals = [v for v in vals if v is not None]
            extra = [self.ev(a, rows[0] if rows else row) for a in args[1:]] if len(args) > 1 else []
            return _agg_values(low, vals, distinct, extra)
        vals = [self.ev(a, row, rows) for a in args]
        f = _GRAPH_FUNCS.get(low)
        try:
            if f is not None:
                return f(self, *vals)
            f = _SCALAR_FUNCS.get(low)
            if f is None:
                raise OpenCypherError(f"Unknown function '{name}'")
            return f(*vals)
        except OpenCypherError:
            raise
        except (TypeError, ValueError, AttributeError, IndexError) as ex:
            raise OpenCypherError(f"Invalid arguments to {name}(): {ex}") from None

    # --------------------------------------------------------- prefetch / out
    def need(self, exprs) -> set:
        out: set = set()
        for e in exprs:
            if e is None:
                continue
            k = id(e)
            if k not in self._need_cache:
                s: set = set()
                _needed_vars(e, s)
                self._need_cache[k] = s
            out |= self._need_cache[k]
        return out

    def prefetch(self, rows: list, exprs) -> None:
        """Batch-load store rows for every element whose properties `exprs` will read."""
        if self.store is None or not rows:
            return
        names = self.need(exprs)
        if not names:
            return
        els = []
        for r in rows:
            for n in names:
                v = r.get(n)
                if isinstance(v, (Vertex, Edge)):
                    els.append(v)
        if els:
            _prefetch_objs(self.g, els)

    def out(self, x, cache: dict):
        """Convert an internal value to a returned one (Node / Rel / Path)."""
        if isinstance(x, Vertex):
            key = ("v", int(x))
            if key not in cache:
                cache[key] = self.node(x)
            return cache[key]
        if isinstance(x, Edge):
            return self.rel(x)
        if isinstance(x, CPath):
            return Path([self.out(n, cache) for n in x.nodes], [self.out(r, cache) for r in x.rels])
        if isinstance(x, (list, tuple)):
            return [self.out(i, cache) for i in x]
        if isinstance(x, dict):
            return {k: self.out(v, cache) for k, v in x.items()}
        return x

    def props_of(self, el) -> dict:
        """All non-null stored properties of an element (one cached row read)."""
        store, g, sc = self.store, self.g, self.sc
        if store is None:
            return {k: v for k in _all_keys(g, el) if (v := _prop(g, el, k)) is not None}
        tn, id_ = sc.type_name(g, el), sc.get_id(g, el)
        if not tn or id_ is None:
            return {}
        row = store.row("vertex" if isinstance(el, Vertex) else "edge", tn, id_)
        return {k: v for k, v in row.items()
                if v is not None and k not in CORE_KEYS and k != store.id_column}

    def node(self, v) -> Node:
        sc = self.sc
        return Node(sc.get_id(self.g, v), sc.label_names(self.g, v), self.props_of(v))

    def rel(self, e) -> Rel:
        sc, g = self.sc, self.g
        return Rel(sc.get_id(g, e), sc.type_name(g, e), sc.get_id(g, e.source()),
                   sc.get_id(g, e.target()), self.props_of(e))


def _link() -> None:
    """Bind the names this module uses from modules loaded after it (they import this one).
    Called once by the package __init__, after every module is loaded."""
    global OpenCypher, _GRAPH_FUNCS
    from .graph_functions import _GRAPH_FUNCS
    from .engine import OpenCypher
