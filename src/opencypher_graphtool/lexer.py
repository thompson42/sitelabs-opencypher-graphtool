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

"""openCypher: lexer."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any
from .errors import OpenCypherSyntaxError

_TOKEN_RE = re.compile(r"""
  (?P<ws>\s+|//[^\n]*|/\*.*?\*/)
 |(?P<float>\d+\.\d+(?:[eE][+-]?\d+)?|\d+[eE][+-]?\d+)
 |(?P<int>0[xX][0-9a-fA-F]+|\d+)
 |(?P<str>'(?:\\.|[^'\\])*'|"(?:\\.|[^"\\])*")
 |(?P<bt>`(?:``|[^`])*`)
 |(?P<param>\$[A-Za-z_]\w*)
 |(?P<id>[A-Za-z_]\w*)
 |(?P<op><>|!=|<=|>=|=~|\+=|->|<-|\.\.|--|[-+*/%^=<>()\[\]{},:.|;])
""", re.X | re.S)

_ESC = {"n": "\n", "t": "\t", "r": "\r", "b": "\b", "f": "\f", "\\": "\\", "'": "'", '"': '"'}


def _unescape(s: str) -> str:
    def sub(m):
        c = m.group(1)
        if c in _ESC:
            return _ESC[c]
        if c[0] in "uU":
            return chr(int(c[1:], 16))
        return c
    return re.sub(r"\\(u[0-9a-fA-F]{4}|U[0-9a-fA-F]{8}|.)", sub, s)


@dataclass
class Tok:
    kind: str       # int float str id bt param op eof
    val: Any
    start: int
    end: int


def _lex(text: str) -> list[Tok]:
    out, pos = [], 0
    while pos < len(text):
        m = _TOKEN_RE.match(text, pos)
        if not m:
            raise OpenCypherSyntaxError(f"unexpected character {text[pos]!r} at offset {pos}")
        pos2, k = m.end(), m.lastgroup
        if k != "ws":
            raw = m.group(k)
            if k == "int":
                val = int(raw, 16) if raw[:2] in ("0x", "0X") else int(raw)
            elif k == "float":
                val = float(raw)
            elif k == "str":
                val = _unescape(raw[1:-1])
            elif k == "bt":
                val = raw[1:-1].replace("``", "`")
            elif k == "param":
                val = raw[1:]
            else:
                val = raw
            out.append(Tok(k, val, pos, pos2))
        pos = pos2
    out.append(Tok("eof", None, len(text), len(text)))
    return out
