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

"""openCypher: parser (recursive descent)."""
from __future__ import annotations

from .errors import OpenCypherSyntaxError
from .lexer import Tok, _lex
from .ast_nodes import Call, Create, Delete, Item, Match, Merge, PNode, PPart, PRel, Proj, Query, Remove, SetC, Single, Unwind

# =========================================================================== #
# openCypher: parser (recursive descent)
# =========================================================================== #
_CLAUSE_WORDS = {"MATCH", "OPTIONAL", "WITH", "RETURN", "UNWIND", "CREATE", "MERGE", "SET",
                 "REMOVE", "DELETE", "DETACH", "CALL", "UNION", "WHERE", "ORDER", "SKIP",
                 "LIMIT", "ON", "YIELD"}


class _Parser:
    def __init__(self, text: str):
        self.text, self.toks, self.i = text, _lex(text), 0

    # -- token helpers ------------------------------------------------------ #
    @property
    def tok(self) -> Tok:
        return self.toks[self.i]

    def peek(self, n: int = 1) -> Tok:
        return self.toks[min(self.i + n, len(self.toks) - 1)]

    def err(self, msg: str):
        t = self.tok
        near = self.text[t.start:t.start + 20] or "end of query"
        raise OpenCypherSyntaxError(f"{msg} (near {near!r}, offset {t.start})")

    def is_op(self, v: str, tok: Tok | None = None) -> bool:
        t = tok or self.tok
        return t.kind == "op" and t.val == v

    def is_kw(self, v: str, tok: Tok | None = None) -> bool:
        t = tok or self.tok
        return t.kind == "id" and t.val.upper() == v

    def accept_op(self, v: str) -> bool:
        if self.is_op(v):
            self.i += 1
            return True
        return False

    def accept_kw(self, *vs: str) -> str | None:
        for v in vs:
            if self.is_kw(v):
                self.i += 1
                return v
        return None

    def expect_op(self, v: str):
        if not self.accept_op(v):
            self.err(f"expected {v!r}")

    def expect_kw(self, v: str):
        if not self.accept_kw(v):
            self.err(f"expected {v}")

    def name(self) -> str:
        t = self.tok
        if t.kind in ("id", "bt"):
            self.i += 1
            return t.val
        self.err("expected a name")

    # -- query -------------------------------------------------------------- #
    def parse(self) -> Query:
        explain = bool(self.accept_kw("EXPLAIN"))
        parts = [self.single()]
        union_all = None
        while self.accept_kw("UNION"):
            a = bool(self.accept_kw("ALL"))
            if union_all is not None and union_all != a:
                self.err("cannot mix UNION and UNION ALL")
            union_all = a
            parts.append(self.single())
        self.accept_op(";")
        if self.tok.kind != "eof":
            self.err("unexpected input")
        return Query(parts, bool(union_all), explain)

    def single(self) -> Single:
        clauses = []
        while True:
            if self.is_kw("MATCH") or (self.is_kw("OPTIONAL") and self.is_kw("MATCH", self.peek())):
                clauses.append(self.match())
            elif self.is_kw("WITH"):
                clauses.append(self.projection("with"))
            elif self.is_kw("RETURN"):
                clauses.append(self.projection("return"))
                break
            elif self.accept_kw("UNWIND"):
                e = self.expr()
                self.expect_kw("AS")
                clauses.append(Unwind(e, self.name()))
            elif self.accept_kw("CREATE"):
                clauses.append(Create(self.patterns()))
            elif self.accept_kw("MERGE"):
                clauses.append(self.merge())
            elif self.accept_kw("SET"):
                clauses.append(SetC(self.set_items()))
            elif self.accept_kw("REMOVE"):
                clauses.append(Remove(self.remove_items()))
            elif self.is_kw("DELETE") or self.is_kw("DETACH"):
                detach = bool(self.accept_kw("DETACH"))
                self.expect_kw("DELETE")
                exprs = [self.expr()]
                while self.accept_op(","):
                    exprs.append(self.expr())
                clauses.append(Delete(exprs, detach))
            elif self.accept_kw("CALL"):
                clauses.append(self.call())
            else:
                break
        if not clauses:
            self.err("expected a clause (MATCH, RETURN, CREATE ...)")
        return Single(clauses)

    # -- clauses ------------------------------------------------------------ #
    def match(self) -> Match:
        optional = bool(self.accept_kw("OPTIONAL"))
        self.expect_kw("MATCH")
        parts = self.patterns()
        where = self.expr() if self.accept_kw("WHERE") else None
        return Match(parts, where, optional)

    def merge(self) -> Merge:
        part = self.pattern_part()
        on_create, on_match = [], []
        while self.is_kw("ON"):
            self.i += 1
            which = self.accept_kw("CREATE", "MATCH")
            if not which:
                self.err("expected CREATE or MATCH after ON")
            self.expect_kw("SET")
            (on_create if which == "CREATE" else on_match).extend(self.set_items())
        return Merge(part, on_create, on_match)

    def set_items(self) -> list:
        items = []
        while True:
            var = self.name()
            if self.accept_op(":"):
                labels = [self.name()]
                while self.accept_op(":"):
                    labels.append(self.name())
                items.append(("labels", var, tuple(labels)))
            elif self.accept_op("+="):
                items.append(("merge", var, self.expr()))
            elif self.is_op("="):
                self.i += 1
                items.append(("replace", var, self.expr()))
            else:
                target = ("var", var)
                while self.accept_op("."):
                    target = ("prop", target, self.name())
                if target[0] != "prop":
                    self.err("expected property, label, = or +=")
                self.expect_op("=")
                items.append(("prop", target, self.expr()))
            if not self.accept_op(","):
                return items

    def remove_items(self) -> list:
        items = []
        while True:
            var = self.name()
            if self.accept_op(":"):
                labels = [self.name()]
                while self.accept_op(":"):
                    labels.append(self.name())
                items.append(("labels", var, tuple(labels)))
            else:
                self.expect_op(".")
                items.append(("prop", var, self.name()))
            if not self.accept_op(","):
                return items

    def call(self) -> Call:
        parts = [self.name()]
        while self.accept_op("."):
            parts.append(self.name())
        args = []
        if self.accept_op("("):
            if not self.is_op(")"):
                args.append(self.expr())
                while self.accept_op(","):
                    args.append(self.expr())
            self.expect_op(")")
        yields, where = [], None
        if self.accept_kw("YIELD"):
            while True:
                f = self.name()
                alias = self.name() if self.accept_kw("AS") else f
                yields.append((f, alias))
                if not self.accept_op(","):
                    break
            if self.accept_kw("WHERE"):
                where = self.expr()
        return Call(".".join(parts), args, yields, where)

    def projection(self, kind: str) -> Proj:
        self.i += 1                                     # WITH / RETURN
        distinct = bool(self.accept_kw("DISTINCT"))
        items, star = [], False
        while True:
            if self.is_op("*"):
                self.i += 1
                star = True
            else:
                a = self.i
                e = self.expr()
                text = self.text[self.toks[a].start:self.toks[self.i - 1].end]
                alias = self.name() if self.accept_kw("AS") else (
                    e[1] if e[0] == "var" else text)
                items.append(Item(e, alias, text))
            if not self.accept_op(","):
                break
        order, skip, limit = [], None, None
        if self.accept_kw("ORDER"):
            self.expect_kw("BY")
            while True:
                e = self.expr()
                desc = False
                d = self.accept_kw("ASC", "ASCENDING", "DESC", "DESCENDING")
                if d and d.startswith("DESC"):
                    desc = True
                order.append((e, desc))
                if not self.accept_op(","):
                    break
        if self.accept_kw("SKIP"):
            skip = self.expr()
        if self.accept_kw("LIMIT"):
            limit = self.expr()
        where = self.expr() if (kind == "with" and self.accept_kw("WHERE")) else None
        return Proj(kind, items, star, distinct, order, skip, limit, where)

    # -- patterns ----------------------------------------------------------- #
    def patterns(self) -> list:
        parts = [self.pattern_part()]
        while self.accept_op(","):
            parts.append(self.pattern_part())
        return parts

    def pattern_part(self) -> PPart:
        path_var = None
        if self.tok.kind in ("id", "bt") and self.is_op("=", self.peek()):
            path_var = self.name()
            self.i += 1
        mode = None
        if self.tok.kind == "id" and self.is_op("(", self.peek()) and \
                self.tok.val.lower() in ("shortestpath", "allshortestpaths"):
            mode = "shortest" if self.tok.val.lower() == "shortestpath" else "all"
            self.i += 2
            part = self.chain()
            self.expect_op(")")
        else:
            part = self.chain()
        part.path_var, part.mode = path_var, mode
        return part

    def chain(self) -> PPart:
        nodes, rels = [self.node()], []
        while self.is_op("-") or self.is_op("<-") or self.is_op("--") or self.is_op("->"):
            rels.append(self.rel())
            nodes.append(self.node())
        return PPart(None, nodes, rels)

    def node(self) -> PNode:
        self.expect_op("(")
        var = None
        if self.tok.kind in ("id", "bt") and not self.is_op(":"):
            var = self.name()
        labels = []
        while self.accept_op(":"):
            labels.append(self.name())
        props = self.props() if self.is_op("{") else None
        self.expect_op(")")
        return PNode(var, tuple(labels), props)

    def props(self):
        if self.tok.kind == "param":
            self.err("expected a map literal")
        self.expect_op("{")
        items = []
        if not self.is_op("}"):
            while True:
                k = self.name()
                self.expect_op(":")
                items.append((k, self.expr()))
                if not self.accept_op(","):
                    break
        self.expect_op("}")
        return items

    def rel(self) -> PRel:
        left = right = False
        if self.accept_op("<-"):
            left = True
        elif self.accept_op("--"):                      # -- or -->
            if self.accept_op(">"):
                return PRel(None, (), None, "out")
            return PRel(None, (), None, "both")
        elif self.accept_op("->"):
            self.err("unexpected '->'")
        else:
            self.expect_op("-")
        if left and self.accept_op("-"):                # <--
            return PRel(None, (), None, "in")
        var, types, props, varlen, lo, hi = None, [], None, False, 1, 1
        if self.accept_op("["):
            if self.tok.kind in ("id", "bt") and not self.is_op(":"):
                var = self.name()
            if self.accept_op(":"):
                types.append(self.name())
                while self.accept_op("|"):
                    self.accept_op(":")
                    types.append(self.name())
            if self.accept_op("*"):
                varlen, lo, hi = True, 1, None
                if self.tok.kind == "int":
                    lo = self.tok.val; self.i += 1
                    if self.accept_op(".."):
                        hi = self.tok.val if self.tok.kind == "int" else None
                        if self.tok.kind == "int":
                            self.i += 1
                    else:
                        hi = lo
                elif self.accept_op(".."):
                    lo = 0 if False else 1
                    if self.tok.kind == "int":
                        hi = self.tok.val; self.i += 1
            if self.is_op("{"):
                props = self.props()
            self.expect_op("]")
        if left:
            self.expect_op("-")
            direction = "in"
        elif self.accept_op("->"):
            direction = "out"
        else:
            self.expect_op("-")
            direction = "both"
        return PRel(var, tuple(types), props, direction, varlen, lo, hi)

    # -- expressions -------------------------------------------------------- #
    def expr(self):
        return self.or_()

    def or_(self):
        a = self.xor()
        while self.accept_kw("OR"):
            a = ("or", a, self.xor())
        return a

    def xor(self):
        a = self.and_()
        while self.accept_kw("XOR"):
            a = ("xor", a, self.and_())
        return a

    def and_(self):
        a = self.not_()
        while self.accept_kw("AND"):
            a = ("and", a, self.not_())
        return a

    def not_(self):
        if self.accept_kw("NOT"):
            return ("not", self.not_())
        return self.cmp()

    _CMP = {"=": "=", "<>": "<>", "!=": "<>", "<": "<", ">": ">", "<=": "<=", ">=": ">="}

    def cmp(self):
        a = self.add()
        parts = []
        while True:
            t = self.tok
            if t.kind == "op" and t.val in self._CMP:
                self.i += 1
                parts.append((self._CMP[t.val], self.add()))
            elif t.kind == "op" and t.val == "<-":                    # `a<-1` means a < -1
                self.toks[self.i:self.i + 1] = [Tok("op", "<", t.start, t.start + 1),
                                                Tok("op", "-", t.start + 1, t.end)]
            elif t.kind == "op" and t.val == "=~":
                self.i += 1
                a = ("regex", a, self.add())
            elif self.is_kw("IS"):
                self.i += 1
                neg = bool(self.accept_kw("NOT"))
                self.expect_kw("NULL")
                a = ("isnull", a, neg)
            elif self.is_kw("IN"):
                self.i += 1
                a = ("in", a, self.add())
            elif self.is_kw("STARTS"):
                self.i += 1; self.expect_kw("WITH"); a = ("strop", "starts", a, self.add())
            elif self.is_kw("ENDS"):
                self.i += 1; self.expect_kw("WITH"); a = ("strop", "ends", a, self.add())
            elif self.is_kw("CONTAINS"):
                self.i += 1; a = ("strop", "contains", a, self.add())
            else:
                break
        if parts:                                       # a < b <= c  ==  a<b AND b<=c
            prev, out = a, None
            for op, rhs in parts:
                c = ("cmp", op, prev, rhs)
                out = c if out is None else ("and", out, c)
                prev = rhs
            return out
        return a

    def add(self):
        a = self.mul()
        while self.tok.kind == "op" and self.tok.val in ("+", "-"):
            op = self.tok.val; self.i += 1
            a = ("bin", op, a, self.mul())
        return a

    def mul(self):
        a = self.pow()
        while self.tok.kind == "op" and self.tok.val in ("*", "/", "%"):
            op = self.tok.val; self.i += 1
            a = ("bin", op, a, self.pow())
        return a

    def pow(self):
        a = self.unary()
        if self.accept_op("^"):
            return ("bin", "^", a, self.pow())
        return a

    def unary(self):
        return _int_literal(self._unary())          # checked once the signs are folded in

    def _unary(self):
        if self.tok.kind == "op" and self.tok.val in ("-", "+"):
            op = self.tok.val; self.i += 1
            e = self._unary()
            if op == "-" and e[0] == "lit" and isinstance(e[1], (int, float)) and not isinstance(e[1], bool):
                return ("lit", -e[1])
            return ("neg", e) if op == "-" else e
        return self.postfix()

    def postfix(self):
        a = self.atom()
        while True:
            if self.is_op("."):
                self.i += 1
                a = ("prop", a, self.name())
            elif self.is_op("["):
                self.i += 1
                if self.accept_op(".."):
                    hi = None if self.is_op("]") else self.expr()
                    self.expect_op("]")
                    a = ("slice", a, None, hi)
                else:
                    lo = self.expr()
                    if self.accept_op(".."):
                        hi = None if self.is_op("]") else self.expr()
                        self.expect_op("]")
                        a = ("slice", a, lo, hi)
                    else:
                        self.expect_op("]")
                        a = ("idx", a, lo)
            elif self.is_op(":") and a[0] == "var" and self.peek().kind in ("id", "bt"):
                labels = []
                while self.accept_op(":"):
                    labels.append(self.name())
                a = ("haslabel", a, tuple(labels))
            else:
                return a

    def atom(self):
        t = self.tok
        if t.kind in ("int", "float", "str"):
            self.i += 1
            if t.kind == "int" and t.val > -_I64_MIN:       # -9223372036854775808 is negated in unary()
                raise OpenCypherSyntaxError(f"integer literal {t.val} is too large for 64 bits")
            return ("lit", t.val)
        if t.kind == "param":
            self.i += 1
            return ("param", t.val)
        if self.is_op("("):
            pat = self._try_pattern_expr()
            if pat is not None:
                return pat
            self.i += 1
            e = self.expr()
            self.expect_op(")")
            return e
        if self.is_op("["):
            return self.list_atom()
        if self.is_op("{"):
            self.i += 1
            items = []
            if not self.is_op("}"):
                while True:
                    k = self.name()
                    self.expect_op(":")
                    items.append((k, self.expr()))
                    if not self.accept_op(","):
                        break
            self.expect_op("}")
            return ("map", items)
        if t.kind == "id":
            up = t.val.upper()
            if up in ("TRUE", "FALSE"):
                self.i += 1
                return ("lit", up == "TRUE")
            if up == "NULL":
                self.i += 1
                return ("lit", None)
            if up == "CASE":
                return self.case()
            if up == "EXISTS" and self.is_op("{", self.peek()):
                self.i += 2
                part = self.pattern_part()
                where = self.expr() if self.accept_kw("WHERE") else None
                self.expect_op("}")
                return ("exists", part, where)
            if self.is_op("(", self.peek()):
                return self.call_expr()
            j = 1                                       # a dotted function name: ns.fn(...)
            while self.is_op(".", self.peek(j)) and self.peek(j + 1).kind == "id":
                j += 2
                if self.is_op("(", self.peek(j)):
                    name = ".".join(self.toks[self.i + n].val for n in range(0, j, 2))
                    self.i += j
                    return self.call_expr(name)
            self.i += 1
            return ("var", t.val)
        if t.kind == "bt":
            self.i += 1
            return ("var", t.val)
        self.err("expected an expression")

    def _try_pattern_expr(self):
        """A parenthesised pattern with at least one relationship is a pattern predicate."""
        save = self.i
        try:
            part = self.chain()
            if part.rels:
                return ("pattern", part)
        except OpenCypherSyntaxError:
            pass
        self.i = save
        return None

    def list_atom(self):
        self.expect_op("[")
        if self.accept_op("]"):
            return ("list", [])
        # list comprehension:  [x IN src WHERE p | e]
        if self.tok.kind in ("id", "bt") and self.is_kw("IN", self.peek()):
            var = self.name(); self.i += 1
            src = self.expr()
            where = self.expr() if self.accept_kw("WHERE") else None
            proj = self.expr() if self.accept_op("|") else None
            self.expect_op("]")
            return ("listcomp", var, src, where, proj)
        # pattern comprehension: [(a)-->(b) WHERE p | e]
        if self.is_op("("):
            save = self.i
            try:
                part = self.chain()
                if part.rels:
                    where = self.expr() if self.accept_kw("WHERE") else None
                    self.expect_op("|")
                    proj = self.expr()
                    self.expect_op("]")
                    return ("patcomp", part, where, proj)
            except OpenCypherSyntaxError:
                pass
            self.i = save
        items = [self.expr()]
        while self.accept_op(","):
            items.append(self.expr())
        self.expect_op("]")
        return ("list", items)

    def case(self):
        self.i += 1
        subject = None if self.is_kw("WHEN") else self.expr()
        whens = []
        while self.accept_kw("WHEN"):
            w = self.expr()
            self.expect_kw("THEN")
            whens.append((w, self.expr()))
        els = self.expr() if self.accept_kw("ELSE") else None
        self.expect_kw("END")
        return ("case", subject, whens, els)

    def call_expr(self, name: str | None = None):
        name = name or self.name()
        self.expect_op("(")
        low = name.lower()
        if low in ("any", "all", "none", "single") and self.tok.kind in ("id", "bt") \
                and self.is_kw("IN", self.peek()):
            var = self.name(); self.i += 1
            src = self.expr()
            where = self.expr() if self.accept_kw("WHERE") else None
            self.expect_op(")")
            return ("quant", low, var, src, where)
        if low == "reduce":
            acc = self.name(); self.expect_op("=")
            init = self.expr(); self.expect_op(",")
            var = self.name(); self.expect_kw("IN")
            src = self.expr(); self.expect_op("|")
            body = self.expr(); self.expect_op(")")
            return ("reduce", acc, init, var, src, body)
        distinct = bool(self.accept_kw("DISTINCT"))
        args = []
        if self.is_op("*") and low == "count":
            self.i += 1
            args = [("star",)]
        elif not self.is_op(")"):
            args.append(self.expr())
            while self.accept_op(","):
                args.append(self.expr())
        self.expect_op(")")
        return ("call", name, args, distinct)


_PARSE_CACHE: dict[str, Query] = {}


def parse(text: str) -> Query:
    q = _PARSE_CACHE.get(text)
    if q is None:
        if len(_PARSE_CACHE) > 500:
            _PARSE_CACHE.clear()
        q = _PARSE_CACHE[text] = _Parser(text).parse()
    return q


def _link() -> None:
    """Bind the names this module uses from modules loaded after it (they import this one).
    Called once by the package __init__, after every module is loaded."""
    global _I64_MIN, _int_literal
    from .values import _I64_MIN, _int_literal
