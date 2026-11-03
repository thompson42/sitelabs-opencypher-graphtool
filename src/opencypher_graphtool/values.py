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

"""openCypher: values, null logic, ordering and the built-in functions."""
from __future__ import annotations

import decimal as _decimal
import math as _math
import random
import time
from dataclasses import dataclass
from graph_tool import Edge
from graph_tool import Graph
from graph_tool import Vertex
from typing import Any
from typing import Callable
from .errors import OpenCypherError, OpenCypherSyntaxError
from .ids import new_id

# =========================================================================== #
# openCypher: values, null logic, ordering, functions
# =========================================================================== #
@dataclass(frozen=True, eq=False)
class CPath:
    """A path value during execution: vertices v0..vn and the edges between them."""
    nodes: tuple
    rels: tuple

    def __eq__(self, o):
        return isinstance(o, CPath) and _hk(self) == _hk(o)

    def __hash__(self):
        return hash(_hk(self))


def _ekey(g: Graph, e: Edge) -> int:
    return int(g.edge_index[e])


def _hk(x: Any, g: Graph | None = None) -> Any:
    """Canonical hashable form (grouping, DISTINCT, equality of containers)."""
    if x is None or isinstance(x, (bool, int, str)):
        return (type(x).__name__, x) if isinstance(x, bool) else x
    if isinstance(x, float):
        return x if x == x else "NaN"
    if isinstance(x, Vertex):
        return ("\0v", int(x))
    if isinstance(x, Edge):
        return ("\0e", x)
    if isinstance(x, CPath):
        return ("\0p", tuple(_hk(n) for n in x.nodes), tuple(_hk(r) for r in x.rels))
    if isinstance(x, (list, tuple)):
        return ("\0l",) + tuple(_hk(i) for i in x)
    if isinstance(x, dict):
        return ("\0m",) + tuple(sorted((k, _hk(v)) for k, v in x.items()))
    return x


def _is_num(x) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool)


def _eq(a: Any, b: Any):
    """openCypher equality with null semantics: True / False / None."""
    if a is None or b is None:
        return None
    if _is_num(a) and _is_num(b):
        return a == b
    if isinstance(a, (list, tuple)) and isinstance(b, (list, tuple)):
        if len(a) != len(b):
            return False
        res = True
        for x, y in zip(a, b):
            r = _eq(x, y)
            if r is False:
                return False
            if r is None:
                res = None
        return res
    if isinstance(a, dict) and isinstance(b, dict):
        if set(a) != set(b):
            return False
        res = True
        for k in a:
            r = _eq(a[k], b[k])
            if r is False:
                return False
            if r is None:
                res = None
        return res
    if type(a) is not type(b) and not (isinstance(a, (Vertex, Edge)) and isinstance(b, (Vertex, Edge))):
        return False
    return _hk(a) == _hk(b)


def _cmp(op: str, a: Any, b: Any):
    if op == "=":
        return _eq(a, b)
    if op == "<>":
        r = _eq(a, b)
        return None if r is None else (not r)
    if a is None or b is None:
        return None
    ok = (_is_num(a) and _is_num(b)) or (isinstance(a, str) and isinstance(b, str)) or \
         (isinstance(a, bool) and isinstance(b, bool))
    if not ok and isinstance(a, (list, tuple)) and isinstance(b, (list, tuple)):
        for x, y in zip(a, b):
            r = _eq(x, y)
            if r is None:
                return None
            if r is False:
                return _cmp(op, x, y)
        a, b = len(a), len(b)
        ok = True
    if not ok:
        return None
    if a != a or b != b:                                  # NaN
        return False
    return {"<": a < b, ">": a > b, "<=": a <= b, ">=": a >= b}[op]


def _rank(x: Any) -> tuple:
    """Total order used by ORDER BY / min / max (nulls last when ascending)."""
    if x is None:
        return (9, 0)
    if isinstance(x, bool):
        return (5, x)
    if _is_num(x):
        return (6, 1, 0) if x != x else (6, 0, x)
    if isinstance(x, str):
        return (4, x)
    if isinstance(x, (list, tuple)):
        return (2, tuple(_rank(i) for i in x))
    if isinstance(x, dict):
        return (0, tuple(sorted((k, _rank(v)) for k, v in x.items())))
    if isinstance(x, Vertex):
        return (1, int(x))
    if isinstance(x, Edge):
        return (1, 1 << 40, int(x.source()), int(x.target()))
    if isinstance(x, CPath):
        return (3, tuple(_rank(n) for n in x.nodes))
    return (8, str(x))


def _truth(x: Any):
    if x is None or isinstance(x, bool):
        return x
    raise OpenCypherError(f"expected a boolean, got {type(x).__name__}")


def _and(a, b):
    if a is False or b is False:
        return False
    if a is None or b is None:
        return None
    return True


def _or(a, b):
    if a is True or b is True:
        return True
    if a is None or b is None:
        return None
    return False


_I64_MIN, _I64_MAX = -(1 << 63), (1 << 63) - 1


def _i64(v: Any) -> Any:
    """openCypher integers are 64-bit: an integer result outside that range is an error (Python's
    are unbounded, so without this the error only appeared when Cassandra refused the value)."""
    if isinstance(v, int) and not isinstance(v, bool) and not _I64_MIN <= v <= _I64_MAX:
        raise OpenCypherError(f"integer overflow: {v} is outside the 64-bit range")
    return v


def _arith(op: str, a: Any, b: Any):
    if a is None or b is None:
        return None
    if op == "+":
        if isinstance(a, str) or isinstance(b, str):
            if isinstance(a, (str, int, float)) and isinstance(b, (str, int, float)) \
                    and not isinstance(a, bool) and not isinstance(b, bool):
                return _tostr(a) + _tostr(b)
            raise OpenCypherError("cannot add a string and a non-scalar")
        if isinstance(a, list) or isinstance(b, list):
            return (a if isinstance(a, list) else [a]) + (b if isinstance(b, list) else [b])
    if not (_is_num(a) and _is_num(b)):
        raise OpenCypherError(f"cannot apply {op!r} to {type(a).__name__} and {type(b).__name__}")
    if op == "+": return _i64(a + b)
    if op == "-": return _i64(a - b)
    if op == "*": return _i64(a * b)
    if op == "/":
        if isinstance(a, int) and isinstance(b, int):
            if b == 0:
                raise OpenCypherError("/ by zero")
            q = abs(a) // abs(b)
            return _i64(q if (a >= 0) == (b >= 0) else -q)      # MIN / -1
        if b == 0:
            return _math.nan if a == 0 or a != a else _math.copysign(_math.inf, a)
        return a / b
    if op == "%":
        if isinstance(a, int) and isinstance(b, int):
            if b == 0:
                raise OpenCypherError("/ by zero")
            return int(_math.fmod(a, b))
        return _math.fmod(a, b) if b != 0 else _math.nan
    if op == "^":
        try:
            return float(a) ** float(b)
        except (OverflowError, ZeroDivisionError):
            return _math.inf
    raise OpenCypherError(f"unknown operator {op}")


def _refuse_maps(props: dict, is_map_field: Callable[[str], bool]) -> None:
    """openCypher property values are scalars or lists of them, never maps. A model field typed
    dict[str, T] (a Cassandra map) is the deliberate exception."""
    for k, v in props.items():
        if isinstance(v, dict) and not is_map_field(k):
            raise ValueError(f"property {k!r}: a map can't be a property value (only a model field "
                             f"typed dict[str, T] holds one); store its entries as separate properties")


def _int_literal(e: tuple) -> tuple:
    """A literal not negated in unary() must fit in 64 bits as it stands."""
    if e[0] == "lit" and isinstance(e[1], int) and not isinstance(e[1], bool) and e[1] > _I64_MAX:
        raise OpenCypherSyntaxError(f"integer literal {e[1]} is too large for 64 bits")
    return e


def _tostr(x: Any) -> str:
    if isinstance(x, bool):
        return "true" if x else "false"
    if isinstance(x, float) and x == int(x) and abs(x) < 1e15:
        return repr(x)
    return str(x)


def _opencypher_repr(x: Any) -> str:
    if x is None:
        return "null"
    if isinstance(x, str):
        return "'" + x.replace("\\", "\\\\").replace("'", "\\'") + "'"
    if isinstance(x, (list, tuple)):
        return "[" + ", ".join(_opencypher_repr(i) for i in x) + "]"
    if isinstance(x, dict):
        return "{" + ", ".join(f"{k}: {_opencypher_repr(v)}" for k, v in x.items()) + "}"
    return _tostr(x)


def _percentile(vals: list, p: float, cont: bool):
    xs = sorted(vals)
    if not xs:
        return None
    if not 0 <= p <= 1:
        raise OpenCypherError("percentile must be between 0.0 and 1.0")
    pos = p * (len(xs) - 1)
    lo = int(_math.floor(pos))
    if not cont:
        return xs[min(len(xs) - 1, max(0, int(_math.ceil(p * len(xs))) - 1))]
    hi = min(lo + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (pos - lo)


def _agg_values(name: str, vals: list, distinct: bool, extra: list):
    """Aggregate over already-evaluated argument values (nulls dropped by the caller)."""
    if distinct:
        seen, out = set(), []
        for v in vals:
            k = _hk(v)
            if k not in seen:
                seen.add(k); out.append(v)
        vals = out
    n = name.lower()
    if n == "count":
        return len(vals)
    if n == "collect":
        return list(vals)
    if not vals:
        return None if n != "sum" else 0
    if n in ("sum", "avg", "stdev", "stdevp", "percentilecont", "percentiledisc"):
        for v in vals:
            if not _is_num(v):
                raise OpenCypherError(f"{n}() needs numbers, got {type(v).__name__}")
    if n == "sum":
        return _i64(sum(vals))
    if n == "avg":
        return sum(vals) / len(vals)
    if n in ("stdev", "stdevp"):
        m = sum(vals) / len(vals)
        d = len(vals) - (1 if n == "stdev" else 0)
        return 0.0 if d <= 0 else _math.sqrt(sum((x - m) ** 2 for x in vals) / d)
    if n in ("percentilecont", "percentiledisc"):
        return _percentile(vals, extra[0], n == "percentilecont")
    if n == "min":
        return min(vals, key=_rank)
    if n == "max":
        return max(vals, key=_rank)
    raise OpenCypherError(f"unknown aggregate {name}")


def _null1(f):
    """Wrap a one-argument function so a null argument gives null."""
    return lambda a: None if a is None else f(a)


def _toint(x):
    if x is None: return None
    if isinstance(x, bool): raise OpenCypherError("toInteger() of a boolean")
    if isinstance(x, (int, float)):
        return None if x != x or x in (_math.inf, -_math.inf) else int(x)
    if isinstance(x, str):
        try:
            return int(x.strip())
        except ValueError:
            try:
                return int(float(x.strip()))
            except ValueError:
                return None
    raise OpenCypherError(f"toInteger() of {type(x).__name__}")


def _tofloat(x):
    if x is None: return None
    if isinstance(x, bool): raise OpenCypherError("toFloat() of a boolean")
    if isinstance(x, (int, float)): return float(x)
    if isinstance(x, str):
        try:
            return float(x.strip())
        except ValueError:
            return None
    raise OpenCypherError(f"toFloat() of {type(x).__name__}")


def _tobool(x):
    if x is None or isinstance(x, bool): return x
    if isinstance(x, str):
        return {"true": True, "false": False}.get(x.strip().lower())
    raise OpenCypherError(f"toBoolean() of {type(x).__name__}")


def _size(x):
    if x is None: return None
    if isinstance(x, (str, list, tuple, dict)): return len(x)
    raise OpenCypherError(f"size() of {type(x).__name__}")


_ROUND_MODES = {"UP": _decimal.ROUND_UP, "DOWN": _decimal.ROUND_DOWN,
                "CEILING": _decimal.ROUND_CEILING, "FLOOR": _decimal.ROUND_FLOOR,
                "HALF_UP": _decimal.ROUND_HALF_UP, "HALF_DOWN": _decimal.ROUND_HALF_DOWN,
                "HALF_EVEN": _decimal.ROUND_HALF_EVEN, "UNNECESSARY": None}
_ROUND_CTX = _decimal.Context(prec=1000)


def _round(x, prec=0, mode="HALF_UP"):
    """As in openCypher: the decimal value of x (as printed, so round(2.675, 2) = 2.68), rounded with a
    standard rounding mode (UP, DOWN, CEILING, FLOOR, HALF_UP, HALF_DOWN, HALF_EVEN, UNNECESSARY);
    the default HALF_UP rounds half away from zero."""
    if x is None or prec is None or mode is None: return None
    rounding = _ROUND_MODES.get(str(mode).upper(), "?")
    if rounding == "?":
        raise OpenCypherError(f"round(): unknown rounding mode {mode!r} (one of {', '.join(_ROUND_MODES)})")
    if isinstance(x, float) and not _math.isfinite(x):
        return x
    d = _decimal.Decimal(repr(x))
    q = d.quantize(_decimal.Decimal(1).scaleb(-int(prec)), rounding=rounding or _decimal.ROUND_DOWN,
                   context=_ROUND_CTX)
    if rounding is None and q != d:
        raise OpenCypherError(f"round(): {x} needs rounding at precision {prec} (mode UNNECESSARY)")
    return float(q)


def _substring(s, start, length=None):
    if s is None or start is None: return None
    return s[start:] if length is None else s[start:start + length]


def _range(a, b, step=1):
    if step == 0: raise OpenCypherError("range(): step cannot be 0")
    return list(range(a, b + (1 if step > 0 else -1), step))


def _coalesce(*xs):
    return next((x for x in xs if x is not None), None)


def _sign(x):
    return None if x is None else (x > 0) - (x < 0)


def _reverse(x):
    if x is None: return None
    return x[::-1] if isinstance(x, str) else list(reversed(x))


def _split(s, d):
    return None if s is None or d is None else (s.split(d) if d else list(s))


def _replace(s, a, b):
    return None if s is None or a is None or b is None else s.replace(a, b)


def _left(s, n): return None if s is None else s[:n]
def _right(s, n): return None if s is None else (s[-n:] if n else "")


def _head(x): return None if x is None else (x[0] if x else None)
def _last(x): return None if x is None else (x[-1] if x else None)
def _tail(x): return None if x is None else list(x[1:])


_SCALAR_FUNCS: dict[str, Callable] = {
    "abs": _null1(abs), "ceil": _null1(lambda x: float(_math.ceil(x))),
    "floor": _null1(lambda x: float(_math.floor(x))), "round": _round, "sign": _sign,
    "sqrt": _null1(lambda x: _math.sqrt(x) if x >= 0 else _math.nan),
    "exp": _null1(_math.exp), "log": _null1(lambda x: _math.log(x) if x > 0 else (-_math.inf if x == 0 else _math.nan)),
    "log10": _null1(lambda x: _math.log10(x) if x > 0 else (-_math.inf if x == 0 else _math.nan)),
    "sin": _null1(_math.sin), "cos": _null1(_math.cos), "tan": _null1(_math.tan),
    "asin": _null1(_math.asin), "acos": _null1(_math.acos), "atan": _null1(_math.atan),
    "atan2": lambda a, b: None if a is None or b is None else _math.atan2(a, b),
    "degrees": _null1(_math.degrees), "radians": _null1(_math.radians),
    "pi": lambda: _math.pi, "e": lambda: _math.e,
    "rand": lambda: RNG.random(),
    "tointeger": _toint, "tofloat": _tofloat, "toboolean": _tobool,
    "tostring": lambda x: None if x is None else _tostr(x),
    "toupper": _null1(str.upper), "tolower": _null1(str.lower),
    "trim": _null1(str.strip), "ltrim": _null1(str.lstrip), "rtrim": _null1(str.rstrip),
    "replace": _replace, "substring": _substring, "left": _left, "right": _right,
    "split": _split, "reverse": _reverse, "size": _size, "head": _head, "last": _last,
    "tail": _tail, "range": _range, "coalesce": _coalesce,
    "isempty": _null1(lambda x: len(x) == 0), "timestamp": lambda: int(time.time() * 1000),
    "randomuuid": lambda: new_id(),
}
RNG = random.Random()          # rand(); RNG.seed(n) for repeatable runs
