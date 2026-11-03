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

"""openCypher: the syntax tree."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# =========================================================================== #
# openCypher: AST
# =========================================================================== #
@dataclass
class PNode:
    var: str | None
    labels: tuple
    props: Any                    # None | [(key, expr)] | expr (a map parameter / expression)


@dataclass
class PRel:
    var: str | None
    types: tuple
    props: Any
    direction: str                # "out" | "in" | "both"
    varlen: bool = False
    lo: int = 1
    hi: int | None = 1            # None = unbounded


@dataclass
class PPart:
    path_var: str | None
    nodes: list
    rels: list
    mode: str | None = None       # None | "shortest" | "all"


@dataclass
class Item:
    expr: Any
    alias: str
    text: str


@dataclass
class Proj:
    kind: str                     # "with" | "return"
    items: list
    star: bool
    distinct: bool
    order: list                   # [(expr, desc)]
    skip: Any
    limit: Any
    where: Any = None


@dataclass
class Match:
    parts: list
    where: Any
    optional: bool


@dataclass
class Unwind:
    expr: Any
    var: str


@dataclass
class Create:
    parts: list


@dataclass
class Merge:
    part: PPart
    on_create: list
    on_match: list


@dataclass
class SetC:
    items: list


@dataclass
class Remove:
    items: list


@dataclass
class Delete:
    exprs: list
    detach: bool


@dataclass
class Call:
    name: str
    args: list
    yields: list                  # [(field, alias)]
    where: Any


@dataclass
class Single:
    clauses: list


@dataclass
class Query:
    parts: list                   # [Single]
    all: bool                     # UNION ALL
    explain: bool = False


_AGGS = {"count", "sum", "avg", "min", "max", "collect", "stdev", "stdevp",
         "percentilecont", "percentiledisc"}


def _has_agg(e: Any) -> bool:
    """Does the expression contain an aggregate function call?"""
    if isinstance(e, tuple):
        if e and e[0] == "call" and e[1].lower() in _AGGS:
            return True
        return any(_has_agg(x) for x in e[1:])
    if isinstance(e, (list,)):
        return any(_has_agg(x) for x in e)
    return False
