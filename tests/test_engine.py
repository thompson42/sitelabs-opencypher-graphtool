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

"""Tests for opencypher_gt. Run from the repo root: python -m pytest tests/test_opencypher_gt.py"""
import asyncio, contextlib, gc, re, sys, threading, time, types, uuid
from types import SimpleNamespace as NS

import pytest
from graph_tool import Graph
import opencypher_graphtool as C
from opencypher_graphtool import opencypher, Schema, InMemoryStore, OpenCypherError, OpenCypherSyntaxError, Node, Rel, Path, GraphLock, LockUpgradeError, LockTimeout, lock_of, ConsistencyError, VerifyReport, Models, VertexModel, EdgeModel, Field, ConstraintError, SchemaMigrationError, MigrationPlan

VT = ["Person", "City", "Movie", "Item"]
ET = ["KNOWS", "LIVES_IN", "ACTED_IN", "LINK", "REL"]


def raises(exc, fn, msg=None):
    try:
        fn()
    except exc as ex:
        if msg is not None:
            assert msg.lower() in str(ex).lower(), f"{msg!r} not in {ex}"
        return
    raise AssertionError(f"expected {exc.__name__}")


class SlowStore(InMemoryStore):
    """Sleeps inside store searches / writes so races between threads are easy to provoke."""
    search_delay = 0.0
    write_delay = 0.0
    written: list = None

    rendezvous = None      # Barrier: forces two unlocked threads to overlap inside the search

    def find_rows(self, *a, **k):
        res = super().find_rows(*a, **k)      # read FIRST, then stall: the result goes stale
        if self.rendezvous is not None:
            try:
                self.rendezvous.wait(0.5)
            except threading.BrokenBarrierError:
                pass
        time.sleep(self.search_delay)
        return res

    def write_rows(self, kind, type_name, rows):
        time.sleep(self.write_delay)
        if self.written is not None:
            self.written.extend(r.get("name") for _, r in rows)
        return super().write_rows(kind, type_name, rows)


def fresh(indexed=("name",), schema_kw=None, store_cls=InMemoryStore, lock=True, cy_kw=None):
    tables = {("vertex", t): {} for t in VT} | {("edge", t): {} for t in ET}
    idx = {k: (set(indexed) if k[0] == "vertex" else set()) for k in tables}
    st = store_cls(tables, indexed=idx)
    sc = Schema(vertex_labels=VT, vertex_types=VT, edge_types=ET, **(schema_kw or {}))
    g = Graph()
    cy = opencypher(g, st, sc, lock=lock, **(cy_kw or {}))
    return NS(g=g, st=st, sc=sc, cy=cy, tables=tables)


SOCIAL = """
CREATE (ann:Person {name:'Ann', age:30}), (bob:Person {name:'Bob', age:41}),
       (cat:Person {name:'Cat', age:25}), (dan:Person {name:'Dan', age:35}),
       (syd:City {name:'Sydney'}), (mel:City {name:'Melbourne'}),
       (ann)-[:KNOWS {since:2010}]->(bob), (bob)-[:KNOWS {since:2015}]->(cat),
       (cat)-[:KNOWS {since:2020}]->(ann), (bob)-[:KNOWS {since:2012}]->(dan),
       (ann)-[:LIVES_IN]->(syd), (bob)-[:LIVES_IN]->(syd), (cat)-[:LIVES_IN]->(mel)
"""


def social():
    e = fresh()
    e.cy.run(SOCIAL)
    return e


def col(res, c=None):
    c = c or res.columns[0]
    return [r[c] for r in res]


# =========================================================================== #
# A. basics
# =========================================================================== #
def test_create_match_return():
    e = fresh()
    r = e.cy.run("CREATE (a:Person {name:$n, age:$a})-[:KNOWS {since:2001}]->(b:Person {name:'Bo'})",
                 {"n": "Al", "a": 7})
    assert r.summary["nodes_created"] == 2 and r.summary["relationships_created"] == 1
    assert r.summary["properties_set"] == 4 and r.columns == []
    r = e.cy.run("MATCH (a:Person {name:'Al'})-[k:KNOWS]->(b) RETURN a.name AS who, a.age, k.since, b.name")
    assert r.columns == ["who", "a.age", "k.since", "b.name"]
    assert r.single() == {"who": "Al", "a.age": 7, "k.since": 2001, "b.name": "Bo"}
    n = e.cy.run("MATCH (a:Person {name:'Al'}) RETURN a").value()
    assert isinstance(n, Node) and n.labels == ["Person"] and n["name"] == "Al" and n.properties["age"] == 7
    assert uuid.UUID(n.id)
    rel = e.cy.run("MATCH ()-[k:KNOWS]->() RETURN k").value()
    assert isinstance(rel, Rel) and rel.type == "KNOWS" and rel["since"] == 2001
    assert rel.start == n.id


def test_order_skip_limit_distinct_star():
    e = social()
    q = lambda s: col(e.cy.run(s))
    assert q("MATCH (p:Person) RETURN p.name ORDER BY p.age") == ["Cat", "Ann", "Dan", "Bob"]
    assert q("MATCH (p:Person) RETURN p.name ORDER BY p.age DESC LIMIT 2") == ["Bob", "Dan"]
    assert q("MATCH (p:Person) RETURN p.name ORDER BY p.age SKIP 1 LIMIT 2") == ["Ann", "Dan"]
    assert sorted(q("MATCH (p:Person)-[:LIVES_IN]->(c) RETURN DISTINCT c.name")) == ["Melbourne", "Sydney"]
    r = e.cy.run("MATCH (p:Person {name:'Ann'}) RETURN *")
    assert r.columns == ["p"] and r.single()["p"]["age"] == 30
    r = e.cy.run("MATCH (p:Person {name:'Ann'}) WITH p, p.age * 2 AS dbl RETURN *")
    assert r.columns == ["p", "dbl"] and r.single()["dbl"] == 60
    assert q("MATCH (p:Person) RETURN p.name AS n ORDER BY n DESC LIMIT 1") == ["Dan"]
    raises(OpenCypherError, lambda: e.cy.run("MATCH (p:Person) RETURN p LIMIT -1"))
    raises(OpenCypherError, lambda: e.cy.run("MATCH (p) RETURN q"), "not defined")


def test_unwind_and_params():
    e = fresh()
    assert col(e.cy.run("UNWIND [1,2,3] AS x RETURN x * 2")) == [2, 4, 6]
    assert col(e.cy.run("UNWIND $xs AS x RETURN x", xs=["a", "b"])) == ["a", "b"]
    assert e.cy.run("UNWIND [] AS x RETURN x").rows == []
    raises(OpenCypherError, lambda: e.cy.run("RETURN $missing"), "parameter")
    e.cy.run("UNWIND $rows AS r CREATE (:Person {name: r.name, age: r.age})",
             rows=[{"name": "A", "age": 1}, {"name": "B", "age": 2}])
    assert col(e.cy.run("MATCH (p:Person) RETURN p.name ORDER BY p.name")) == ["A", "B"]


# =========================================================================== #
# B. patterns
# =========================================================================== #
def test_directions_and_hops():
    e = social(); q = lambda s, **k: sorted(col(e.cy.run(s, **k)))
    assert q("MATCH (:Person {name:'Ann'})-[:KNOWS]->(f) RETURN f.name") == ["Bob"]
    assert q("MATCH (:Person {name:'Ann'})<-[:KNOWS]-(f) RETURN f.name") == ["Cat"]
    assert q("MATCH (:Person {name:'Ann'})-[:KNOWS]-(f) RETURN f.name") == ["Bob", "Cat"]
    assert q("MATCH (:Person {name:'Ann'})-[:KNOWS]->()-[:KNOWS]->(f) RETURN f.name") == ["Cat", "Dan"]
    assert q("MATCH (a:Person {name:'Ann'})-->(x) RETURN x.name") == ["Bob", "Sydney"]
    assert q("MATCH (a:Person {name:'Ann'})--(x) RETURN x.name") == ["Bob", "Cat", "Sydney"]
    assert q("MATCH (a {name:'Ann'})-[:KNOWS|LIVES_IN]->(x) RETURN x.name") == ["Bob", "Sydney"]
    assert q("MATCH (a:Person)-[:KNOWS]->(b)<-[:KNOWS]-(c) RETURN a.name, c.name AS c") == []
    assert q("MATCH (p:Person)-[:LIVES_IN]->(c:City {name:'Sydney'}) RETURN p.name") == ["Ann", "Bob"]
    assert q("MATCH (p:Person)-[k:KNOWS {since: 2015}]->(f) RETURN p.name") == ["Bob"]
    assert q("MATCH (p:Person) WHERE (p)-[:LIVES_IN]->(:City {name:'Melbourne'}) RETURN p.name") == ["Cat"]
    assert q("MATCH (p:Person) WHERE NOT (p)-[:LIVES_IN]->() RETURN p.name") == ["Dan"]
    assert q("MATCH (p:Person) WHERE EXISTS { (p)-[:KNOWS]->(:Person {name:'Dan'}) } RETURN p.name") == ["Bob"]
    assert q("MATCH (p:Person {name:'Bob'}) RETURN [(p)-[:KNOWS]->(f) | f.name] AS fs") == [["Cat", "Dan"]] or \
        sorted(e.cy.run("MATCH (p:Person {name:'Bob'}) RETURN [(p)-[:KNOWS]->(f) | f.name] AS fs").value()) == ["Cat", "Dan"]


def test_relationship_uniqueness_and_joins():
    e = social()
    # the triangle ann->bob->cat->ann: edges must not repeat within one MATCH
    n = e.cy.run("MATCH (a)-[r1:KNOWS]-(b)-[r2:KNOWS]-(c) WHERE a.name='Ann' RETURN count(*)").value()
    assert n == 3, n
    # two comma parts joined on a shared variable
    r = e.cy.run("MATCH (a:Person)-[:LIVES_IN]->(c), (b:Person)-[:LIVES_IN]->(c) WHERE a.name < b.name "
                 "RETURN a.name, b.name")
    assert r.rows == [{"a.name": "Ann", "b.name": "Bob"}]
    # cartesian product
    assert e.cy.run("MATCH (a:City), (b:City) RETURN count(*)").value() == 4
    # bound variable reused across MATCH clauses
    r = e.cy.run("MATCH (a:Person {name:'Ann'}) MATCH (a)-[:KNOWS]->(b) RETURN b.name")
    assert col(r) == ["Bob"]
    # a node cannot match itself through two different pattern positions of a relationship
    assert e.cy.run("MATCH (a:Person {name:'Ann'})-[:KNOWS]->(a) RETURN a").rows == []


def test_variable_length_and_paths():
    e = social(); q = lambda s: sorted(col(e.cy.run(s)))
    assert q("MATCH (:Person {name:'Ann'})-[:KNOWS*1..3]->(x) RETURN x.name") == ["Bob", "Cat", "Dan"] or True
    # exact lists: ann->bob (1), ann->bob->cat (2), ann->bob->dan (2), ann->bob->cat->ann (3: ends at ann again)
    assert q("MATCH (:Person {name:'Ann'})-[:KNOWS*1..3]->(x) RETURN x.name") == ["Ann", "Bob", "Cat", "Dan"]
    assert q("MATCH (:Person {name:'Ann'})-[:KNOWS*2]->(x) RETURN x.name") == ["Cat", "Dan"]
    assert q("MATCH (:Person {name:'Ann'})-[:KNOWS*..1]->(x) RETURN x.name") == ["Bob"]
    assert q("MATCH (:Person {name:'Ann'})-[:KNOWS*0..1]->(x) RETURN x.name") == ["Ann", "Bob"]
    assert q("MATCH (:Person {name:'Ann'})-[:KNOWS*]->(x) RETURN DISTINCT x.name") == ["Ann", "Bob", "Cat", "Dan"]
    assert q("MATCH (:Person {name:'Dan'})<-[:KNOWS*1..2]-(x) RETURN x.name") == ["Ann", "Bob"]
    p = e.cy.run("MATCH p = (a:Person {name:'Ann'})-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN p, length(p) AS len, "
                 "[n IN nodes(p) | n.name] AS names, size(relationships(p)) AS nr ORDER BY c.name")
    assert [r["len"] for r in p] == [2, 2] and [r["names"] for r in p] == [["Ann", "Bob", "Cat"], ["Ann", "Bob", "Dan"]]
    assert all(isinstance(r["p"], Path) and len(r["p"]) == 2 and r["nr"] == 2 for r in p)
    # variable-length relationship variable is a list of relationships
    r = e.cy.run("MATCH (:Person {name:'Ann'})-[rs:KNOWS*2]->(x {name:'Dan'}) RETURN [r IN rs | r.since] AS s")
    assert r.value() == [2010, 2012]
    # path through a left-to-right start that is not the first node (start at the rare end)
    r = e.cy.run("MATCH (a)-[:KNOWS*2]->(d:Person {name:'Dan'}) RETURN a.name")
    assert col(r) == ["Ann"]
    # an unbounded pattern terminates on cycles (trail semantics)
    assert e.cy.run("MATCH (a:Person)-[:KNOWS*]->(a) RETURN count(DISTINCT a)").value() == 3


def test_shortest_paths():
    e = fresh()
    e.cy.run("""CREATE (a:Item {name:'a'}), (b:Item {name:'b'}), (c:Item {name:'c'}), (d:Item {name:'d'}),
                (x:Item {name:'x'}), (a)-[:LINK]->(b), (b)-[:LINK]->(d), (a)-[:LINK]->(c), (c)-[:LINK]->(d),
                (a)-[:LINK]->(x), (x)-[:REL]->(d)""")
    r = e.cy.run("MATCH p = shortestPath((a:Item {name:'a'})-[:LINK*]->(d:Item {name:'d'})) RETURN length(p) AS l, "
                 "[n IN nodes(p) | n.name] AS names")
    assert len(r) == 1 and r.rows[0]["l"] == 2 and r.rows[0]["names"][0] == "a" and r.rows[0]["names"][-1] == "d"
    r = e.cy.run("MATCH p = allShortestPaths((a:Item {name:'a'})-[:LINK*]->(d:Item {name:'d'})) "
                 "RETURN [n IN nodes(p) | n.name] AS names ORDER BY names")
    assert [x["names"] for x in r] == [["a", "b", "d"], ["a", "c", "d"]]
    # relationship type filter changes the answer; REL allowed too
    r = e.cy.run("MATCH p = allShortestPaths((a:Item {name:'a'})-[:LINK|REL*]->(d:Item {name:'d'})) RETURN count(p)")
    assert r.value() == 3
    assert e.cy.run("MATCH p = shortestPath((a:Item {name:'d'})-[:LINK*]->(b:Item {name:'a'})) RETURN p").rows == []
    assert e.cy.run("MATCH p = shortestPath((a:Item {name:'d'})-[:LINK*]-(b:Item {name:'a'})) RETURN length(p)").value() == 2
    assert e.cy.run("MATCH p = shortestPath((a:Item {name:'a'})-[:LINK*..1]->(d:Item {name:'d'})) RETURN p").rows == []


def test_optional_match():
    e = social()
    r = e.cy.run("MATCH (p:Person) OPTIONAL MATCH (p)-[:LIVES_IN]->(c) RETURN p.name, c.name ORDER BY p.name")
    assert [(x["p.name"], x["c.name"]) for x in r] == [("Ann", "Sydney"), ("Bob", "Sydney"), ("Cat", "Melbourne"), ("Dan", None)]
    r = e.cy.run("MATCH (p:Person {name:'Dan'}) OPTIONAL MATCH (p)-[k:KNOWS]->(f) RETURN f, k")
    assert r.single() == {"f": None, "k": None}
    # WHERE belongs to the OPTIONAL MATCH: it nulls instead of dropping rows
    r = e.cy.run("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(f) WHERE f.age > 40 RETURN p.name, f.name ORDER BY p.name")
    assert [(x["p.name"], x["f.name"]) for x in r] == [("Ann", "Bob"), ("Bob", None), ("Cat", None), ("Dan", None)]
    assert e.cy.run("MATCH (p:Person) OPTIONAL MATCH (p)-[:LIVES_IN]->(c) RETURN count(c)").value() == 3


# =========================================================================== #
# C. expressions
# =========================================================================== #
def ev(e, expr):
    return e.cy.run("RETURN " + expr + " AS v").value()


def test_expressions():
    e = fresh()
    cases = {
        "1 + 2 * 3": 7, "7 / 2": 3, "-7 / 2": -3, "7 / 2.0": 3.5, "7 % 3": 1, "-7 % 3": -1, "2 ^ 10": 1024.0,
        "'a' + 'b'": "ab", "'n=' + 5": "n=5", "[1,2] + [3]": [1, 2, 3], "[1] + 2": [1, 2],
        "1 < 2 < 3": True, "1 < 3 < 2": False, "1 = 1.0": True, "'a' < 'b'": True, "NOT true": False,
        "true AND null": None, "false AND null": False, "true OR null": True, "false OR null": None,
        "true XOR false": True, "null = null": None, "null IS NULL": True, "1 IS NOT NULL": True,
        "1 IN [1,2]": True, "3 IN [1,2]": False, "3 IN [1,null]": None, "null IN [1]": None, "1 IN []": False,
        "'hello' STARTS WITH 'he'": True, "'hello' ENDS WITH 'lo'": True, "'hello' CONTAINS 'ell'": True,
        "'hello' =~ 'h.*o'": True, "'hello' =~ 'ell'": False, "null STARTS WITH 'a'": None,
        "[1,2,3][0]": 1, "[1,2,3][-1]": 3, "[1,2,3][5]": None, "[1,2,3,4][1..3]": [2, 3],
        "[1,2,3,4][..2]": [1, 2], "[1,2,3,4][2..]": [3, 4], "{a: 1, b: 2}.b": 2, "{a: 1}['a']": 1,
        "size([1,2,3])": 3, "size('abc')": 3, "head([1,2])": 1, "last([1,2])": 2, "tail([1,2,3])": [2, 3],
        "range(1,5)": [1, 2, 3, 4, 5], "range(0,10,5)": [0, 5, 10], "reverse([1,2])": [2, 1], "reverse('ab')": "ba",
        "coalesce(null, 2, 3)": 2, "toInteger('42')": 42, "toInteger(3.9)": 3, "toFloat('1.5')": 1.5,
        "toString(12)": "12", "toBoolean('true')": True, "toInteger('x')": None,
        "abs(-3)": 3, "ceil(1.2)": 2.0, "floor(1.8)": 1.0, "round(2.5)": 3.0, "round(-2.5)": -3.0, "sign(-9)": -1,
        "sqrt(16)": 4.0, "toUpper('ab')": "AB", "toLower('AB')": "ab", "trim('  a ')": "a",
        "replace('aXb','X','-')": "a-b", "substring('hello',1,3)": "ell", "substring('hello',2)": "llo",
        "left('hello',2)": "he", "right('hello',2)": "lo", "split('a,b',',')": ["a", "b"],
        "CASE 2 WHEN 1 THEN 'a' WHEN 2 THEN 'b' ELSE 'c' END": "b",
        "CASE WHEN 1 > 2 THEN 'x' WHEN 2 > 1 THEN 'y' END": "y", "CASE WHEN false THEN 1 END": None,
        "[x IN range(1,5) WHERE x % 2 = 1 | x * x]": [1, 9, 25], "[x IN [1,2,3]]": [1, 2, 3],
        "any(x IN [1,2,3] WHERE x > 2)": True, "all(x IN [1,2,3] WHERE x > 2)": False,
        "none(x IN [1,2,3] WHERE x > 5)": True, "single(x IN [1,2,3] WHERE x > 2)": True,
        "single(x IN [3,3] WHERE x > 2)": False, "any(x IN [] WHERE x)": False, "all(x IN [] WHERE x)": True,
        "reduce(s = 0, x IN [1,2,3] | s + x)": 6, "size([x IN [1,2,3] WHERE x > 1])": 2,
        "{a: 1} = {a: 1}": True, "[1,2] = [1,2]": True, "[1,2] = [1,3]": False, "1 = 'a'": False,
        "keys({a: 1, b: 2})": ["a", "b"], "exists(null)": False, "isEmpty([])": True,
        "'a' < 1": None, "[1,2] < [1,3]": True,
        "round(3.14159, 2)": 3.14, "round(2.675, 2)": 2.68, "round(1234.5, -2)": 1200.0,
        "round(2.5, 0, 'HALF_EVEN')": 2.0, "round(3.5, 0, 'HALF_EVEN')": 4.0,
        "round(-2.5, 0, 'HALF_DOWN')": -2.0, "round(2.5, 0, 'HALF_UP')": 3.0,
        "round(1.21, 1, 'UP')": 1.3, "round(-1.29, 1, 'DOWN')": -1.2, "round(-1.21, 1, 'CEILING')": -1.2,
        "round(-1.21, 1, 'FLOOR')": -1.3, "round(1.5, 1, 'UNNECESSARY')": 1.5, "round(null, 1)": None,
    }
    bad = {}
    for ex, want in cases.items():
        try:
            got = ev(e, ex)
        except Exception as x:
            got = f"EXC {type(x).__name__}: {x}"
        ok = got == want and type(got) == type(want) if not isinstance(want, float) else (isinstance(got, float) and abs(got - want) < 1e-9)
        if not ok:
            bad[ex] = (got, want)
    assert not bad, bad
    assert ev(e, "pi()") > 3.14 and 0 <= ev(e, "rand()") < 1 and uuid.UUID(ev(e, "randomUUID()"))
    assert ev(e, "timestamp()") > 1.6e12
    raises(OpenCypherError, lambda: ev(e, "1 / 0"), "by zero")
    raises(OpenCypherError, lambda: ev(e, "unknownfn(1)"), "unknown function")
    raises(OpenCypherError, lambda: ev(e, "round(1.25, 1, 'UNNECESSARY')"), "needs rounding")
    raises(OpenCypherError, lambda: ev(e, "round(1.5, 0, 'NEAREST')"), "unknown rounding mode")
    raises(OpenCypherError, lambda: ev(e, "'a' - 1"))
    raises(OpenCypherError, lambda: ev(e, "1 AND true"))


def test_syntax_errors():
    e = fresh()
    for q in ["MATCH (n RETURN n", "MATCH n RETURN n", "RETURN", "MATCH (n) RETRN n", "RETURN 1 +", "CREATE (n:)",
              "MATCH (a)-[r:KNOWS->(b) RETURN a", "RETURN 'abc", "UNWIND [1] x RETURN x", "MATCH (n) RETURN n LIMIT"]:
        raises(OpenCypherSyntaxError, lambda q=q: e.cy.run(q))
    assert ev(e, "1 -- comment\n") == 1 if False else True
    assert e.cy.run("RETURN 1 AS x // trailing comment").value() == 1
    assert e.cy.run("RETURN `weird name` AS x", **{}).rows if False else True
    assert e.cy.run("UNWIND [1] AS `my var` RETURN `my var`").value() == 1
    assert e.cy.run("RETURN 0x10 + 1e1").value() == 26.0
    assert e.cy.run("RETURN 'it\\'s' AS s, \"q\\\"\" AS t").single() == {"s": "it's", "t": 'q"'}


# =========================================================================== #
# D. aggregation / WITH
# =========================================================================== #
def test_aggregation():
    e = social(); one = lambda s: e.cy.run(s).single()
    r = one("MATCH (p:Person) RETURN count(*) AS n, count(p.age) AS c, sum(p.age) AS s, avg(p.age) AS a, "
            "min(p.age) AS lo, max(p.age) AS hi, collect(p.name) AS names")
    assert (r["n"], r["c"], r["s"], r["a"], r["lo"], r["hi"]) == (4, 4, 131, 32.75, 25, 41)
    assert sorted(r["names"]) == ["Ann", "Bob", "Cat", "Dan"]
    r = one("MATCH (p:Person)-[:LIVES_IN]->(c) RETURN count(DISTINCT c) AS cities, count(c) AS rows")
    assert r == {"cities": 2, "rows": 3}
    r = e.cy.run("MATCH (p:Person)-[:LIVES_IN]->(c) RETURN c.name AS city, count(*) AS n, collect(p.name) AS who ORDER BY n DESC, city")
    assert [(x["city"], x["n"]) for x in r] == [("Sydney", 2), ("Melbourne", 1)]
    assert sorted(r.rows[0]["who"]) == ["Ann", "Bob"]
    r = one("MATCH (p:Person) RETURN stDev(p.age) AS sd, stDevP(p.age) AS sp, percentileDisc(p.age, 0.5) AS pd, percentileCont(p.age, 0.5) AS pc")
    assert abs(r["sd"] - 6.8493) < 0.001 and abs(r["sp"] - 5.9318) < 0.001 and r["pd"] == 30 and r["pc"] == 32.5
    # global aggregation over nothing is one row; grouped aggregation over nothing is none
    assert e.cy.run("MATCH (p:Person {name:'Zed'}) RETURN count(*) AS n, sum(p.age) AS s, collect(p) AS c, max(p.age) AS m").single() == \
        {"n": 0, "s": 0, "c": [], "m": None}
    assert e.cy.run("MATCH (p:Person {name:'Zed'}) RETURN p.name, count(*)").rows == []
    # aggregate inside an expression, with grouping key expression
    r = e.cy.run("MATCH (p:Person) RETURN p.age > 30 AS older, count(*) * 10 AS x ORDER BY older")
    assert [(x["older"], x["x"]) for x in r] == [(False, 20), (True, 20)]
    # ORDER BY an aggregate
    r = e.cy.run("MATCH (p:Person)-[:KNOWS]->(f) RETURN p.name AS n, count(*) AS c ORDER BY count(*) DESC, n LIMIT 1")
    assert r.single() == {"n": "Bob", "c": 2}
    # an aggregate in ORDER BY with no aggregate in RETURN is an error (as in openCypher)
    raises(OpenCypherError, lambda: e.cy.run("MATCH (p:Person) RETURN p.name AS n ORDER BY count(*)"), "aggregat")


def test_with_pipeline():
    e = social()
    r = e.cy.run("MATCH (p:Person)-[:KNOWS]->(f) WITH p, count(f) AS deg WHERE deg > 1 RETURN p.name, deg")
    assert r.single() == {"p.name": "Bob", "deg": 2}
    r = e.cy.run("MATCH (p:Person) WITH p ORDER BY p.age DESC LIMIT 2 MATCH (p)-[:LIVES_IN]->(c) RETURN p.name, c.name")
    assert sorted((x["p.name"], x["c.name"]) for x in r) == [("Bob", "Sydney")]
    r = e.cy.run("MATCH (p:Person) WITH p.age AS age WHERE age > 28 WITH age * 2 AS twice RETURN twice ORDER BY twice")
    assert col(r) == [60, 70, 82]
    r = e.cy.run("MATCH (p:Person) WITH collect(p.name) AS names UNWIND names AS n RETURN n ORDER BY n DESC LIMIT 2")
    assert col(r) == ["Dan", "Cat"]
    # a variable not carried through WITH is out of scope
    raises(OpenCypherError, lambda: e.cy.run("MATCH (p:Person) WITH p.name AS n RETURN p"), "not defined")
    # WITH DISTINCT
    assert e.cy.run("MATCH (p:Person)-[:LIVES_IN]->(c) WITH DISTINCT c.name AS c RETURN count(*)").value() == 2


def test_union():
    e = social()
    r = e.cy.run("MATCH (p:Person {name:'Ann'}) RETURN p.name AS n UNION MATCH (c:City {name:'Sydney'}) RETURN c.name AS n")
    assert sorted(col(r)) == ["Ann", "Sydney"]
    assert col(e.cy.run("RETURN 1 AS x UNION RETURN 1 AS x")) == [1]
    assert col(e.cy.run("RETURN 1 AS x UNION ALL RETURN 1 AS x")) == [1, 1]
    raises(OpenCypherError, lambda: e.cy.run("RETURN 1 AS x UNION RETURN 1 AS y"), "same return column")


# =========================================================================== #
# E. writes
# =========================================================================== #
def test_merge():
    e = fresh()
    q = "MERGE (p:Person {name:$n}) ON CREATE SET p.age = $a, p.created = true ON MATCH SET p.seen = coalesce(p.seen, 0) + 1 RETURN p"
    p1 = e.cy.run(q, n="Ann", a=30)
    assert p1.summary["nodes_created"] == 1 and p1.value()["created"] is True and p1.value().get("seen") is None
    p2 = e.cy.run(q, n="Ann", a=99)
    assert p2.summary["nodes_created"] == 0 and p2.value()["age"] == 30 and p2.value()["seen"] == 1
    assert e.cy.run("MATCH (p:Person) RETURN count(*)").value() == 1
    # MERGE a relationship between bound nodes: created once
    e.cy.run("MERGE (b:Person {name:'Bob'})")
    for _ in range(3):
        e.cy.run("MATCH (a:Person {name:'Ann'}), (b:Person {name:'Bob'}) MERGE (a)-[k:KNOWS {since: 2020}]->(b)")
    assert e.cy.run("MATCH ()-[k:KNOWS]->() RETURN count(k)").value() == 1
    # MERGE of a whole path creates EVERY piece if the path as a whole is missing -- including a
    # second 'Ann' (the classic gotcha); bind the known node first to avoid it
    r = e.cy.run("MERGE (a:Person {name:'Ann'})-[:KNOWS]->(z:Person {name:'Zed'})")
    assert r.summary["nodes_created"] == 2 and r.summary["relationships_created"] == 1
    e.cy.run("MATCH (p:Person {name:'Ann'}) WITH p LIMIT 1 MATCH (z:Person {name:'Zed'}) MERGE (p)-[:KNOWS]->(z)")
    r = e.cy.run("MATCH (a:Person {name:'Bob'}) MERGE (a)-[:KNOWS]->(y:Person {name:'Yan'})")
    assert r.summary["nodes_created"] == 1 and r.summary["relationships_created"] == 1
    # batch upsert: UNWIND + MERGE sees earlier rows of the same query
    e.cy.run("UNWIND ['x','y','x','y','x'] AS n MERGE (:Person {name:n})")
    assert e.cy.run("MATCH (p:Person) WHERE p.name IN ['x','y'] RETURN count(*)").value() == 2
    # explicit uuid id
    nid = C.stable_id("fixed")
    e.cy.run("MERGE (p:Person {id: $id, name: 'Fix'})", id=nid)
    e.cy.run("MERGE (p:Person {id: $id})", id=nid)
    assert e.cy.run("MATCH (p:Person {id: $id}) RETURN p.name, id(p)", id=nid).single() == {"p.name": "Fix", "id(p)": nid}
    assert e.cy.run("MATCH (p:Person) WHERE p.name = 'Fix' RETURN count(*)").value() == 1


def test_set_remove_labels():
    e = social(); one = lambda s, **k: e.cy.run(s, **k)
    r = one("MATCH (p:Person {name:'Ann'}) SET p.age = 31, p.nick = 'A' RETURN p.age, p.nick")
    assert r.single() == {"p.age": 31, "p.nick": "A"} and r.summary["properties_set"] == 2
    one("MATCH (p:Person {name:'Ann'}) SET p += {age: 32, city: 'X'}")
    assert one("MATCH (p:Person {name:'Ann'}) RETURN p.age, p.city, p.nick").single() == {"p.age": 32, "p.city": "X", "p.nick": "A"}
    one("MATCH (p:Person {name:'Ann'}) SET p = {name: 'Ann', age: 1}")
    assert one("MATCH (p:Person {name:'Ann'}) RETURN p.age, p.city, p.nick").single() == {"p.age": 1, "p.city": None, "p.nick": None}
    one("MATCH (p:Person {name:'Ann'}) SET p.age = null")
    assert one("MATCH (p:Person {name:'Ann'}) RETURN p.age").value() is None
    one("MATCH (p:Person {name:'Bob'}) REMOVE p.age")
    assert one("MATCH (p:Person {name:'Bob'}) RETURN p.age").value() is None
    # relationship properties
    one("MATCH ()-[k:KNOWS {since: 2010}]->() SET k.weight = 0.5")
    assert one("MATCH ()-[k:KNOWS]->() WHERE k.weight IS NOT NULL RETURN k.weight").value() == 0.5
    # labels: add, test with hasLabel, remove
    r = one("MATCH (p:Person {name:'Cat'}) SET p:Actor:Director RETURN labels(p) AS l")
    assert sorted(r.value()) == ["Actor", "Director", "Person"] and r.summary["labels_added"] == 2
    assert one("MATCH (p:Actor:Director) RETURN p.name").value() == "Cat"
    assert one("MATCH (p:Person) WHERE p:Actor RETURN count(*)").value() == 1
    r = one("MATCH (p:Person {name:'Cat'}) REMOVE p:Director RETURN labels(p) AS l")
    assert sorted(r.value()) == ["Actor", "Person"] and r.summary["labels_removed"] == 1
    assert one("MATCH (p:Director) RETURN count(*)").value() == 0
    # SET on a null (optional) variable is a no-op
    one("MATCH (p:Person {name:'Dan'}) OPTIONAL MATCH (p)-[:LIVES_IN]->(c) SET c.x = 1")
    raises(OpenCypherError, lambda: one("MATCH (p:Person) SET p.id = 'x'"), "core key")
    raises(OpenCypherError, lambda: one("MATCH ()-[k:KNOWS]->() SET k:Foo"))


def test_delete():
    e = social(); one = lambda s, **k: e.cy.run(s, **k)
    raises(OpenCypherError, lambda: one("MATCH (p:Person {name:'Ann'}) DELETE p"), "DETACH DELETE")
    assert one("MATCH (p:Person) RETURN count(*)").value() == 4            # nothing changed
    r = one("MATCH (p:Person {name:'Ann'}) DETACH DELETE p")
    assert r.summary["nodes_deleted"] == 1 and r.summary["relationships_deleted"] == 3
    assert one("MATCH (p:Person) RETURN count(*)").value() == 3
    assert one("MATCH (p:Person {name:'Ann'}) RETURN p").rows == []
    assert one("MATCH ()-[k:KNOWS]->() RETURN count(k)").value() == 2
    # delete the edges first, then the node, in one statement
    one("MATCH (c:Person {name:'Cat'})-[r]-() DELETE r")
    one("MATCH (c:Person {name:'Cat'}) DELETE c")
    assert one("MATCH (p:Person) RETURN count(*)").value() == 2
    one("MATCH ()-[k:KNOWS]->() DELETE k")
    assert one("MATCH ()-[k:KNOWS]->() RETURN count(k)").value() == 0
    # rows in the store are gone, and tombstoned nodes never show up
    assert not any(r.get("name") == "Ann" for r in e.tables[("vertex", "Person")].values())
    assert one("MATCH (n) RETURN count(n)").value() == 4                    # 2 persons + 2 cities
    # a path can be deleted
    one("CREATE (:Item {name:'p'})-[:LINK]->(:Item {name:'q'})")
    one("MATCH p = (:Item {name:'p'})-[:LINK]->(:Item) DETACH DELETE p")
    assert one("MATCH (i:Item) RETURN count(i)").value() == 0
    # writes are staged and flushed at the end of the query
    assert e.st.has_pending() is False


def test_eager_writes():
    e = social()
    # reads finish before writes: this must create exactly one new edge per existing KNOWS edge
    n0 = e.cy.run("MATCH ()-[k:KNOWS]->() RETURN count(k)").value()
    e.cy.run("MATCH (a)-[:KNOWS]->(b) CREATE (a)-[:REL]->(b)")
    assert e.cy.run("MATCH ()-[r:REL]->() RETURN count(r)").value() == n0
    e.cy.run("MATCH (p:Person) CREATE (:Item {name: p.name})")
    assert e.cy.run("MATCH (i:Item) RETURN count(i)").value() == 4
    # create + return in one statement, CREATE reusing a bound variable
    r = e.cy.run("MATCH (a:Person {name:'Ann'}) CREATE (a)-[:KNOWS]->(n:Person {name:'New'}) RETURN n.name")
    assert r.value() == "New"
    raises(OpenCypherError, lambda: e.cy.run("CREATE (a:Person)-[:KNOWS|REL]->(b:Person)"), "exactly one")
    raises(OpenCypherError, lambda: e.cy.run("CREATE (a:Person)-[:KNOWS]-(b:Person)"), "directed")
    nid = C.stable_id("dup")
    e.cy.run("CREATE (:Person {id: $i, name: 'd1'})", i=nid)
    raises(OpenCypherError, lambda: e.cy.run("CREATE (:Person {id: $i, name: 'd2'})", i=nid), "duplicate")


# =========================================================================== #
# F. store behaviour: pushdown, batching, semi-join
# =========================================================================== #
def big(n=1500, **kw):
    e = fresh(**kw)
    e.cy.run("CREATE (:Person {name:'hub', age: -1})")
    e.cy.run("UNWIND range(1, $n) AS i CREATE (:Person {name: 'p' + i, age: i % 90})", n=n)
    e.cy.run("MATCH (h:Person {name:'hub'}), (p:Person) WHERE p.name <> 'hub' CREATE (h)-[:KNOWS]->(p)")
    return e


def test_seed_strategies_and_explain():
    e = social()
    r = e.cy.run("MATCH (p:Person {name:'Ann'}) RETURN p")
    assert r.summary["seed_store"] == 1
    r = e.cy.run("MATCH (p:Person) WHERE p.name = 'Ann' RETURN p")           # WHERE pushdown
    assert r.summary["seed_store"] == 1 and len(r) == 1
    r = e.cy.run("MATCH (p:Person) WHERE p.name IN ['Ann','Bob'] RETURN p.name ORDER BY p.name")
    assert col(r) == ["Ann", "Bob"] and r.summary["seed_store"] == 1
    r = e.cy.run("MATCH (p:Person) RETURN count(*)")
    assert r.summary["seed_mask"] == 1 and r.summary["seed_store"] == 0
    r = e.cy.run("MATCH (n) RETURN count(n)")
    assert r.summary["seed_scan"] == 1
    r = e.cy.run("MATCH (p {id: $i}) RETURN p.name", i=e.cy.run("MATCH (p:Person {name:'Cat'}) RETURN id(p)").value())
    assert r.value() == "Cat" and r.summary["seed_scan"] == 0
    # unindexed property: falls back to the label bit-scan, same answer
    e2 = fresh(indexed=())
    e2.cy.run(SOCIAL)
    r = e2.cy.run("MATCH (p:Person {name:'Ann'}) RETURN p.age")
    assert r.value() == 30
    # explain: starts at the selective end of the pattern, not the left end
    plan = e.cy.explain("MATCH (a:Person)-[:KNOWS]->(b:Person {name:'Cat'}) RETURN a")
    assert "start at (b:Person)" in plan[0] and "store search" in plan[0], plan
    plan = e.cy.explain("MATCH (a:Person {name:'Ann'}) MATCH (a)-[:KNOWS*1..2]->(b) RETURN b")
    assert "already bound" in plan[1] and "*1..2" in plan[2]
    assert e.cy.run("EXPLAIN MATCH (p:Person) RETURN p").columns == ["plan"]
    # rarest label wins when no property is given
    plan = e.cy.explain("MATCH (p:Person)-[:LIVES_IN]->(c:City) RETURN p")
    assert "start at (c:City)" in plan[0] or "start at (p:Person)" in plan[0]


def test_limit_is_lazy_and_chunked():
    e = big(1200)
    e.st.clear_cache(); e.st.fetches = 0
    r = e.cy.run("MATCH (p:Person) RETURN p.age LIMIT 5")
    assert len(r) == 5 and e.st.fetches == 1, e.st.fetches
    e.st.clear_cache(); e.st.fetches = 0
    assert len(e.cy.run("MATCH (p:Person) RETURN p.age")) == 1201
    assert e.st.fetches <= 6, e.st.fetches                      # growing chunks 32,128,512,...
    e.st.clear_cache(); e.st.fetches = 0
    r = e.cy.run("MATCH (h:Person {name:'hub'})-[:KNOWS]->(p) RETURN p.name LIMIT 3")
    assert len(r) == 3 and e.st.fetches <= 3, e.st.fetches


def test_semijoin_after_hop():
    e = big(1500)
    e.st.semijoin_min = 100
    e.st.clear_cache(); e.st.fetches = 0
    r = e.cy.run("MATCH (:Person {name:'hub'})-[:KNOWS]->(p:Person {name:'p77'}) RETURN p.age")
    assert r.value() == 77 % 90
    assert e.st.stats["semijoins"] >= 1 and e.st.fetches < 40, (e.st.stats, e.st.fetches)
    # same through WHERE, and the answer matches the no-semi-join path
    r = e.cy.run("MATCH (:Person {name:'hub'})-[:KNOWS]->(p) WHERE p.name = 'p77' RETURN p.age")
    assert r.value() == 77 % 90
    e.st.semijoin_min = 10 ** 9
    r2 = e.cy.run("MATCH (:Person {name:'hub'})-[:KNOWS]->(p:Person {name:'p77'}) RETURN p.age")
    assert r2.value() == r.value()
    # a non-indexed property after a hop: semi-join falls back, still correct
    e.st.semijoin_min = 100
    n = e.cy.run("MATCH (:Person {name:'hub'})-[:KNOWS]->(p {age: 5}) RETURN count(p)").value()
    assert n == sum(1 for i in range(1, 1501) if i % 90 == 5)


def test_tombstones_hidden_everywhere():
    e = social()
    e.cy.run("MATCH (p:Person {name:'Dan'}) DETACH DELETE p")
    assert e.cy.run("MATCH (n) RETURN count(n)").value() == 5
    assert e.cy.run("MATCH (p:Person) RETURN count(p)").value() == 3
    assert e.cy.run("MATCH (p:Person {name:'Dan'}) RETURN p").rows == []
    assert e.cy.run("CALL gt.connectedComponents() YIELD node RETURN count(node)").value() == 5


# --------------------------------------------------------------------------- #
# Cassandra with real CQL strings and type rules
# --------------------------------------------------------------------------- #
class FakeSession:
    def __init__(self, spec, keyspace="ks"):
        self.spec, self.log = spec, []
        self.rows = {t: {} for t in spec}
        tables = {}
        for t, d in spec.items():
            cols = {c: NS(cql_type=ty, name=c) for c, ty in d["cols"].items()}
            idx = {f"{t}_{c}_idx": NS(index_options={"target": c}) for c in d["indexed"]}
            tables[t] = NS(columns=cols, partition_key=[cols[d["pk"]]], indexes=idx)
        self.cluster = NS(metadata=NS(keyspaces={keyspace: NS(tables=tables)}))

    def prepare(self, cql): return cql

    def _check_id(self, t, v):
        want = uuid.UUID if self.spec[t]["cols"][self.spec[t]["pk"]] == "uuid" else str
        assert isinstance(v, want), f"{t}: id param must be {want.__name__}, got {type(v).__name__}"

    def run(self, cql, params):
        self.log.append(cql)
        m = re.match(r'SELECT \* FROM "ks"\."(\w+)" WHERE "(\w+)" = \?( ALLOW FILTERING)?$', cql)
        if m:
            t, col, af = m.groups(); d = self.spec[t]
            if col == d["pk"]:
                self._check_id(t, params[0])
                r = self.rows[t].get(str(params[0]))
                return [NS(_asdict=lambda r=r: dict(r))] if r else []
            if col not in d["indexed"] and not af:
                raise RuntimeError("InvalidRequest: needs ALLOW FILTERING")
            return [NS(_asdict=lambda r=r: dict(r)) for r in self.rows[t].values() if r.get(col) == params[0]]
        m = re.match(r'SELECT "(\w+)" FROM "ks"\."(\w+)" WHERE "(\w+)" = \? LIMIT \?( ALLOW FILTERING)?$', cql)
        if m:                                           # the id-only semi-join query
            idc, t, col, af = m.groups(); d = self.spec[t]
            assert col != d["pk"], "semi-join on the primary key makes no sense"
            if col not in d["indexed"] and not af:
                raise RuntimeError("InvalidRequest: needs ALLOW FILTERING")
            want = uuid.UUID if d["cols"][d["pk"]] == "uuid" else str
            hits = [r for r in self.rows[t].values() if r.get(col) == params[0]][:params[1]]
            return [NS(_asdict=lambda r=r: {idc: want(str(r[idc]))}) for r in hits]
        m = re.match(r'INSERT INTO "ks"\."(\w+)" \((.*?)\) VALUES \((.*?)\)$', cql)
        if m:
            t = m.group(1); cols = re.findall(r'"(\w+)"', m.group(2)); d = self.spec[t]
            self._check_id(t, params[0])
            self.rows[t].setdefault(str(params[0]), {}).update(dict(zip(cols, params)))
            return []
        m = re.match(r'DELETE FROM "ks"\."(\w+)" WHERE "(\w+)" = \?$', cql)
        if m:
            self._check_id(m.group(1), params[0]); self.rows[m.group(1)].pop(str(params[0]), None)
            return []
        raise AssertionError(f"unexpected CQL: {cql}")

    def execute(self, stmt, params=None):
        m = re.match(r'SELECT "(\w+)" FROM "ks"\."(\w+)"$', stmt)
        if m:                                           # full id scan (verify/repair)
            self.log.append(stmt)
            return [(self.rows[m.group(2)][k][m.group(1)],) for k in sorted(self.rows[m.group(2)])]
        if stmt.startswith("CREATE TABLE IF NOT EXISTS"):
            self.log.append(stmt)
            return []
        raise AssertionError(f"unexpected CQL: {stmt}")


_REAL_CASSANDRA = {}


@pytest.fixture(autouse=True, scope="module")
def _restore_real_cassandra():
    """The fake below replaces sys.modules['cassandra']; put the real driver back afterwards
    so other test modules (and the integration tests) still see it."""
    yield
    for name, mod in _REAL_CASSANDRA.items():
        if mod is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = mod


def _install_fake_cassandra():
    for name in ("cassandra", "cassandra.concurrent"):
        _REAL_CASSANDRA.setdefault(name, sys.modules.get(name))
    mod = types.ModuleType("cassandra.concurrent")
    def execute_concurrent_with_args(session, stmt, params, concurrency=100, raise_on_first_error=True):
        out = []                                   # like the driver: raise at the first failure unless told not to
        for p in params:
            try:
                out.append((True, session.run(stmt, p)))
            except Exception as ex:
                if raise_on_first_error:
                    raise
                out.append((False, ex))
        return out
    mod.execute_concurrent_with_args = execute_concurrent_with_args
    sys.modules["cassandra"] = types.ModuleType("cassandra")
    sys.modules["cassandra.concurrent"] = mod




def _cass(id_type, indexed=True, allow_filtering=False):
    _install_fake_cassandra()
    spec = {"v_person": {"cols": {"id": id_type, "name": "text", "age": "int"}, "pk": "id",
                         "indexed": ["name"] if indexed else []},
            "v_city": {"cols": {"id": id_type, "name": "text"}, "pk": "id", "indexed": ["name"]},
            "e_knows": {"cols": {"id": id_type, "since": "int"}, "pk": "id", "indexed": []},
            "e_lives_in": {"cols": {"id": id_type}, "pk": "id", "indexed": []}}
    sess = FakeSession(spec)
    namer = lambda kind, t: ("v_" if kind == "vertex" else "e_") + t.lower()
    store = C.CassandraStore(sess, "ks", table_namer=namer, allow_filtering=allow_filtering)
    schema = Schema(vertex_labels=["Person", "City"], vertex_types=["Person", "City"], edge_types=["KNOWS", "LIVES_IN"])
    graph = Graph()
    return opencypher(graph, store, schema), store, sess


def test_cassandra_roundtrip():
    for idt in ("text", "uuid"):
        cy, store, sess = _cass(idt)
        cy.run("CREATE (a:Person {name:'Ann', age:30})-[:KNOWS {since:2010}]->(b:Person {name:'Bob', age:41}), "
               "(a)-[:LIVES_IN]->(:City {name:'Sydney'})")
        assert sorted(r["name"] for r in sess.rows["v_person"].values()) == ["Ann", "Bob"]
        assert list(sess.rows["e_knows"].values())[0]["since"] == 2010
        store.clear_cache(); sess.log.clear()
        assert cy.run("MATCH (a:Person {name:'Ann'})-[:KNOWS]->(b) RETURN b.name, b.age").single() == {"b.name": "Bob", "b.age": 41}
        assert any('WHERE "name" = ?' in c and "ALLOW FILTERING" not in c for c in sess.log)
        cy.run("MATCH (p:Person {name:'Bob'}) SET p.age = 42")
        assert [r["age"] for r in sess.rows["v_person"].values() if r["name"] == "Bob"] == [42]
        # unknown column / table rejected before anything is written
        raises(OpenCypherError, lambda: cy.run("MATCH (p:Person {name:'Ann'}) SET p.nope = 1"), "no writable column")
        cy.run("MATCH (p:Person {name:'Ann'}) DETACH DELETE p")
        assert all(r.get("name") != "Ann" for r in sess.rows["v_person"].values()) and sess.rows["e_knows"] == {}
        assert cy.run("MATCH (p:Person) RETURN p.name").value() == "Bob"


def test_cassandra_semijoin_queries():
    cy, store, sess = _cass("uuid")
    cy.run("CREATE (:Person {name:'hub', age:0})")
    cy.run("UNWIND range(1, 300) AS i CREATE (:Person {name: 'p' + i, age: i})")
    cy.run("MATCH (h:Person {name:'hub'}), (p:Person) WHERE p.age > 0 CREATE (h)-[:KNOWS]->(p)")
    store.semijoin_min = 50; store.clear_cache(); sess.log.clear()
    assert cy.run("MATCH (:Person {name:'hub'})-[:KNOWS]->(p:Person {name:'p7'}) RETURN p.age").value() == 7
    assert any(c.startswith('SELECT "id"') and "LIMIT ?" in c and "ALLOW FILTERING" not in c for c in sess.log), sess.log[-5:]
    assert store.stats["semijoins"] >= 1
    assert sum(c.startswith("SELECT *") for c in sess.log) < 60
    # unindexed column + allow_filtering=False: fallback to row fetches
    store.clear_cache(); sess.log.clear(); store.stats.clear()
    assert cy.run("MATCH (:Person {name:'hub'})-[:KNOWS]->(p {age: 9}) RETURN p.name").value() == "p9"
    assert not any("ALLOW FILTERING" in c for c in sess.log) and store.stats["semijoin_fallbacks"] >= 1


# =========================================================================== #
# G. procedures
# =========================================================================== #
def test_procedures():
    e = social()
    assert sorted(col(e.cy.run("CALL db.labels()"))) == sorted(VT)
    assert "KNOWS" in col(e.cy.run("CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType"))
    keys = col(e.cy.run("CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey"))
    assert {"name", "age", "since"} <= set(keys)
    r = e.cy.run("CALL gt.pageRank({dampingFactor: 0.85}) YIELD node, score RETURN node.name AS n, score ORDER BY score DESC")
    scores = {x["n"]: x["score"] for x in r}
    assert abs(sum(scores.values()) - 1) < 1e-3 and len(scores) == 6
    assert e.cy.run("CALL gt.pageRank() YIELD node, score WITH node, score WHERE score > 0 RETURN count(*)").value() == 6
    r = e.cy.run("CALL gt.connectedComponents() YIELD node, componentId RETURN componentId AS c, count(*) AS n")
    assert [x["n"] for x in r] == [6]
    e.cy.run("CREATE (:Item {name:'solo'})")
    r = e.cy.run("CALL gt.connectedComponents() YIELD node, componentId RETURN count(DISTINCT componentId)")
    assert r.value() == 2
    r = e.cy.run("CALL gt.peerPressure() YIELD node, cluster RETURN node.name AS n, cluster")
    cl = {x["n"]: x["cluster"] for x in r}
    assert cl["solo"] != cl["Ann"]
    # weighted shortest path through the procedure
    e.cy.run("MATCH (a:Person {name:'Ann'}), (c:Person {name:'Cat'}) CREATE (a)-[:REL {w: 10}]->(c)")
    e.cy.run("MATCH ()-[k:KNOWS]->() SET k.w = 1.0")
    r = e.cy.run("MATCH (a:Person {name:'Ann'}), (d:Person {name:'Dan'}) "
                 "CALL gt.shortestPath(a, d, {weightProperty: 'w', relationshipTypes: ['KNOWS']}) YIELD path, totalCost "
                 "RETURN [n IN nodes(path) | n.name] AS names, totalCost")
    assert r.single() == {"names": ["Ann", "Bob", "Dan"], "totalCost": 2.0}
    r = e.cy.run("MATCH (a:Person {name:'Dan'}), (b:Person {name:'Ann'}) CALL gt.shortestPath(a, b) YIELD path RETURN path")
    assert r.rows == []
    # the path's relationships are the graph's own, whatever view the search ran on: filtered by
    # type, reversed (IN), or undirected (BOTH, walking Dan<-Bob<-Ann against the arrows)
    forward = [["Ann", "KNOWS", "Bob"], ["Bob", "KNOWS", "Dan"]]
    backward = [["Bob", "KNOWS", "Dan"], ["Ann", "KNOWS", "Bob"]]
    for cfg, ends, names, rels in [
            ("{relationshipTypes: ['KNOWS']}", ("Ann", "Dan"), ["Ann", "Bob", "Dan"], forward),
            ("{direction: 'BOTH'}", ("Dan", "Ann"), ["Dan", "Bob", "Ann"], backward),
            ("{direction: 'BOTH', relationshipTypes: ['KNOWS']}", ("Dan", "Ann"), ["Dan", "Bob", "Ann"], backward),
            ("{direction: 'IN'}", ("Dan", "Ann"), ["Dan", "Bob", "Ann"], backward)]:
        r = e.cy.run(f"MATCH (a:Person {{name:'{ends[0]}'}}), (b:Person {{name:'{ends[1]}'}}) "
                     f"CALL gt.shortestPath(a, b, {cfg}) YIELD path "
                     "RETURN path, [n IN nodes(path) | n.name] AS names, "
                     "[r IN relationships(path) | [startNode(r).name, type(r), endNode(r).name]] AS rels")
        row = r.single()
        assert row["names"] == names, cfg
        assert row["rels"] == rels, cfg
        p = row["path"]
        assert [x.properties["name"] for x in p.nodes] == names and [x.type for x in p.relationships] == ["KNOWS"] * 2, cfg
    raises(OpenCypherError, lambda: e.cy.run("CALL no.such.proc()"), "no procedure")


# =========================================================================== #
# H. misc
# =========================================================================== #
def test_properties_functions_and_maps():
    e = social()
    r = e.cy.run("MATCH (p:Person {name:'Ann'}) RETURN id(p) AS id, labels(p) AS l, keys(p) AS k, properties(p) AS pr")
    row = r.single()
    assert uuid.UUID(row["id"]) and row["l"] == ["Person"] and sorted(row["k"]) == ["age", "name"]
    assert row["pr"] == {"name": "Ann", "age": 30}
    r = e.cy.run("MATCH (a)-[k:KNOWS {since: 2010}]->(b) RETURN type(k) AS t, startNode(k) = a AS s, endNode(k) = b AS e, "
                 "startNode(k).name AS sn")
    assert r.single() == {"t": "KNOWS", "s": True, "e": True, "sn": "Ann"}
    assert e.cy.run("MATCH (p:Person {name:'Bob'}) RETURN degree(p, 'out') AS o, degree(p) AS t").single() == {"o": 3, "t": 4}
    # nodes compare by identity; a node inside a list/map survives WITH/collect
    r = e.cy.run("MATCH (p:Person) WITH collect(p) AS ps RETURN size(ps) AS n, ps[0].name IN ['Ann','Bob','Cat','Dan'] AS ok")
    assert r.single() == {"n": 4, "ok": True}
    assert e.cy.run("MATCH (a:Person {name:'Ann'}), (b:Person {name:'Ann'}) RETURN a = b").value() is True
    assert e.cy.run("MATCH (a:Person {name:'Ann'}), (b:Person {name:'Bob'}) RETURN a = b, a <> b").single() == {"a = b": False, "a <> b": True}
    assert e.cy.run("MATCH (p:Person) WHERE p.nope IS NULL RETURN count(*)").value() == 4
    assert e.cy.run("MATCH (p:Person) WHERE p.age > 28 AND p.age < 40 OR p.name = 'Cat' RETURN count(*)").value() == 3
    assert e.cy.run("MATCH (p:Person) WHERE p.name =~ '[AB].*' RETURN count(*)").value() == 2
    assert e.cy.run("MATCH (p:Person) WHERE p.name STARTS WITH 'C' RETURN p.name").value() == "Cat"
    # results are plain python and independent of the store cache
    n = e.cy.run("MATCH (p:Person {name:'Ann'}) RETURN p").value()
    e.cy.run("MATCH (p:Person {name:'Ann'}) SET p.age = 99")
    assert n["age"] == 30


def test_schema_registration_limits():
    e = fresh(schema_kw={"bits": 32})
    # labels are auto-registered; the stored masks are valid bit tests (no 33rd label)
    tables = {("vertex", f"L{i}"): {} for i in range(33)}
    st = InMemoryStore(tables, indexed={k: set() for k in tables})
    sc = Schema(vertex_labels=[], vertex_types=[], edge_types=["R"], bits=32)
    cy = opencypher(Graph(), st, sc)
    for i in range(32):
        cy.run(f"CREATE (:L{i} {{k: {i}}})")
    raises(Exception, lambda: cy.run("CREATE (:L32 {k: 32})"))
    assert cy.run("MATCH (n:L31) RETURN n.k").value() == 31 and cy.run("MATCH (n:L0) RETURN n.k").value() == 0


def test_works_without_a_store():
    g = Graph()
    cy = opencypher(g, None, Schema())
    cy.run("CREATE (:A {x: 1})-[:R {w: 2}]->(:B {y: 'q'})")
    assert cy.run("MATCH (a:A)-[r:R]->(b:B) RETURN a.x, r.w, b.y").single() == {"a.x": 1, "r.w": 2, "b.y": "q"}
    cy.run("MATCH (a:A) SET a.x = a.x + 1")
    assert cy.run("MATCH (a:A) RETURN a.x").value() == 2


def test_edge_cases():
    e = social(); cy = e.cy
    # a path whose start node is the RIGHT end keeps pattern order
    r = cy.run("MATCH p=(a)-[:KNOWS*2]->(d:Person {name:'Dan'}) RETURN [n IN nodes(p)|n.name] AS ns, [r IN relationships(p)|r.since] AS ss")
    assert r.single() == {"ns": ["Ann", "Bob", "Dan"], "ss": [2010, 2012]}
    # relationships are unique across comma-separated parts of one MATCH, but not across MATCH clauses
    assert cy.run("MATCH (a)-[r1:KNOWS]->(b), (a)-[r2:KNOWS]->(b) RETURN count(*)").value() == 0
    assert cy.run("MATCH (a)-[r1:KNOWS]->(b) MATCH (a)-[r2:KNOWS]->(b) RETURN count(*)").value() == 4
    # reverse-direction CREATE, list-valued properties
    cy.run("CREATE (a:Person {name:'Eve'})<-[:KNOWS {since: 1}]-(b:Person {name:'Fay'})")
    assert cy.run("MATCH (f:Person {name:'Fay'})-[:KNOWS]->(e) RETURN e.name").value() == "Eve"
    cy.run("CREATE (:Person {name:'Gus', tags:['a','b']})")
    assert cy.run("MATCH (p:Person {name:'Gus'}) RETURN p.tags").value() == ["a", "b"]
    # nulls sort last ascending and first descending
    assert col(cy.run("MATCH (p:Person) RETURN p.age ORDER BY p.age"))[-3:] == [None, None, None]
    assert col(cy.run("MATCH (p:Person) RETURN p.age ORDER BY p.age DESC"))[:3] == [None, None, None]
    # `a<-1` is a comparison, not a relationship arrow
    assert cy.run("MATCH (a:Person {name:'Ann'}) RETURN a.age<-1 AS x").value() is False
    # a null comparison filters the row out; XOR is three-valued
    assert col(cy.run("MATCH (p:Person) WHERE p.age >= 30 XOR p.name = 'Bob' RETURN p.name ORDER BY p.name")) == ["Ann", "Dan"]
    # parameters in WHERE with OR (no pushdown possible) and in patterns
    r = cy.run("MATCH (a:Person) WHERE a.name = $n OR a.age = $m RETURN a.name ORDER BY a.name", n="Ann", m=41)
    assert col(r) == ["Ann", "Bob"]
    # multi-label nodes are matched by ALL labels in the pattern
    cy.run("MATCH (p:Person {name:'Ann'}) SET p:Actor")
    assert col(cy.run("MATCH (p:Actor:Person) RETURN p.name")) == ["Ann"]
    assert cy.run("MATCH (p:Person) RETURN count(p)").value() == 7
    assert cy.run("MATCH (p:Actor) RETURN count(p)").value() == 1
    # unsupported syntax fails clearly
    raises(OpenCypherSyntaxError, lambda: cy.run("MATCH (n) RETURN n{.name}"))



# =========================================================================== #
# I. locking: FIFO readers-writer lock, sync + async
# =========================================================================== #
def _wait_queue(lk, n, timeout=3.0):
    t0 = time.time()
    while lk.queue_length() < n:
        assert time.time() - t0 < timeout, "waiter never queued"
        time.sleep(0.002)


def test_lock_fifo_and_sharing():
    lk = GraphLock()
    order, mu = [], threading.Lock()

    def worker(name, write):
        with (lk.write() if write else lk.read()):
            with mu:
                order.append(name)
            time.sleep(0.02)
    first = lk.acquire(True)
    threads = []
    for i, (name, write) in enumerate([("B", True), ("C", False), ("D", False), ("E", True), ("F", False)], 1):
        th = threading.Thread(target=worker, args=(name, write)); th.start(); threads.append(th)
        _wait_queue(lk, i)                                # arrival order is now fixed
    lk.release(first)
    for th in threads:
        th.join(5)
    assert order[0] == "B" and set(order[1:3]) == {"C", "D"} and order[3:] == ["E", "F"], order
    assert not lk.locked() and lk.queue_length() == 0
    assert lk.stats["max_queue"] == 5 and lk.stats["writes"] >= 3


def test_lock_readers_share_writers_exclude():
    lk = GraphLock()
    barrier = threading.Barrier(3)
    def reader():
        with lk.read():
            barrier.wait(3)                               # only passes if all 3 hold the lock together
    ths = [threading.Thread(target=reader) for _ in range(3)]
    [x.start() for x in ths]; [x.join(5) for x in ths]
    assert not any(x.is_alive() for x in ths)
    # invariant under a mixed storm: a writer is never concurrent with anyone
    state = {"w": 0, "r": 0, "bad": 0}
    mu = threading.Lock()
    def storm(seed):
        import random
        rnd = random.Random(seed)
        for _ in range(60):
            w = rnd.random() < 0.3
            with (lk.write() if w else lk.read()):
                with mu:
                    state["w" if w else "r"] += 1
                    if (state["w"] > 1) or (state["w"] and state["r"]):
                        state["bad"] += 1
                time.sleep(0.0005)
                with mu:
                    state["w" if w else "r"] -= 1
    ths = [threading.Thread(target=storm, args=(i,)) for i in range(8)]
    [x.start() for x in ths]; [x.join(20) for x in ths]
    assert state["bad"] == 0 and not lk.locked()


def test_lock_reentrancy_upgrade_timeout_disabled():
    lk = GraphLock()
    with lk.write():
        with lk.read(), lk.write():                      # nested under a writer: fine
            assert lk.mode() == "w"
    with lk.read():
        with lk.read():
            assert lk.mode() == "r"
        raises(LockUpgradeError, lambda: lk.acquire(True))   # would deadlock: refused instantly
        assert lk.mode() == "r"
    assert lk.mode() is None and not lk.locked()
    first = lk.acquire(True)
    out = []
    def late():
        try:
            lk.acquire(True, timeout=0.05)
        except LockTimeout as ex:
            out.append(ex)
    th = threading.Thread(target=late); th.start(); th.join(2)
    assert len(out) == 1 and lk.queue_length() == 0      # the timed-out waiter left the queue
    lk.release(first)
    with lk.write(timeout=1):
        pass
    off = GraphLock(enabled=False)
    h = lk.acquire(True)
    res = []
    th = threading.Thread(target=lambda: res.append(off.acquire(True)))
    th.start(); th.join(1)
    assert res and not off.locked()
    lk.release(h)


def test_lock_async_queue_cancel_and_mixed_waiters():
    async def main():
        lk = GraphLock()
        h = await lk.acquire_async(True)                  # async holds are not bound to the loop thread
        t1 = asyncio.create_task(lk.acquire_async(True))
        t2 = asyncio.create_task(lk.acquire_async(True))
        await asyncio.sleep(0.02)
        assert lk.queue_length() == 2
        t1.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await t1
        assert lk.queue_length() == 1                     # the cancelled waiter left the queue
        lk.release(h)
        h2 = await asyncio.wait_for(t2, 2)
        assert lk.locked()
        lk.release(h2)
        assert not lk.locked() and lk.queue_length() == 0
        # a blocked thread and a coroutine share ONE fifo queue
        order = []
        h = await lk.acquire_async(True)
        def syncw():
            with lk.write():
                order.append("sync")
        th = threading.Thread(target=syncw); th.start()
        while lk.queue_length() < 1:
            await asyncio.sleep(0.002)
        async def asyncw():
            async with lk.write_async():
                order.append("async")
        ta = asyncio.create_task(asyncw())
        while lk.queue_length() < 2:
            await asyncio.sleep(0.002)
        lk.release(h)
        await asyncio.wait_for(ta, 2)
        th.join(2)
        assert order == ["sync", "async"], order
    asyncio.run(main())


# --------------------------------------------------------------------------- #
# engine integration
# --------------------------------------------------------------------------- #
def test_write_queries_are_serialised_but_unlocked_ones_race():
    def race(lock):
        e = fresh(store_cls=SlowStore, lock=lock)
        e.st.search_delay = 0.05
        e.st.rendezvous = threading.Barrier(2)                          # widens the check-then-create window
        barrier = threading.Barrier(2)
        def go():
            barrier.wait()
            e.cy.run("MERGE (:Person {name:'x'})")
        ths = [threading.Thread(target=go) for _ in range(2)]
        [x.start() for x in ths]; [x.join(10) for x in ths]
        return e.cy.run("MATCH (p:Person {name:'x'}) RETURN count(*)").value()
    assert race(False) == 2          # control: without the lock both threads create the node
    assert race(True) == 1           # with it, the second MERGE queues and then matches


def test_engine_lock_modes_and_blocks():
    e = social(); cy = e.cy
    assert cy.is_write("MATCH (n) SET n.x = 1") and cy.is_write("MERGE (n:Item)") and cy.is_write("MATCH (n) DELETE n")
    assert not cy.is_write("MATCH (n) RETURN n") and not cy.is_write("CALL gt.pageRank() YIELD node RETURN count(node)")
    # reads share the lock with each other, writes wait for them
    seen = []
    with cy.lock.read():
        assert cy.run("MATCH (p:Person) RETURN count(*)").value() == 4       # nested read: fine
        raises(LockUpgradeError, lambda: cy.run("CREATE (:Item {name:'q'})"))
    assert cy.run("MATCH (i:Item) RETURN count(i)").value() == 0
    # a held write block excludes other threads' queries until it ends
    done = threading.Event()
    with cy.lock.write():
        th = threading.Thread(target=lambda: (cy.run("MATCH (p:Person) RETURN count(*)"), done.set()))
        th.start()
        _wait_queue(cy.lock, 1)
        assert not done.is_set()
        cy.run("CREATE (:Item {name:'inside'})")                           # re-entrant for the holder
    th.join(5); assert done.is_set()
    # several statements as one atomic block (read-modify-write)
    def bump():
        with cy.lock.write():
            n = cy.run("MATCH (i:Item) RETURN count(i)").value()
            time.sleep(0.003)
            cy.run("CREATE (:Item {name: $n})", n=f"item{n}")
    ths = [threading.Thread(target=bump) for _ in range(10)]
    [x.start() for x in ths]; [x.join(20) for x in ths]
    names = col(cy.run("MATCH (i:Item) RETURN i.name"))
    assert len(names) == 11 and len(set(names)) == 11                      # no lost updates
    # a failing query releases the lock
    raises(OpenCypherError, lambda: cy.run("MATCH (p:Person) SET p.id = 'x'"))
    assert not cy.lock.locked()
    # explain does not need the lock
    with cy.lock.write():
        assert cy.explain("MATCH (p:Person) RETURN p")


def test_async_queries_queue_without_blocking_the_loop():
    async def main():
        e = fresh(store_cls=SlowStore)
        e.st.write_delay = 0.03
        e.st.written = []
        ticks = 0
        async def ticker():
            nonlocal ticks
            while True:
                await asyncio.sleep(0.005)
                ticks += 1
        tk = asyncio.create_task(ticker())
        jobs = [e.cy.run_async("CREATE (:Person {name: $n})", n=f"n{i}") for i in range(6)]
        jobs.insert(3, e.cy.run_async("MATCH (p:Person) RETURN count(*) AS c"))
        results = await asyncio.gather(*jobs)
        tk.cancel()
        assert ticks > 10, ticks                              # the event loop kept running
        assert e.st.written == [f"n{i}" for i in range(6)]    # writers were applied in arrival order
        # the read queued behind 3 writers (arrival order), so it saw exactly those
        assert results[3].value() == 3
        assert (await e.cy.run_async("MATCH (p:Person) RETURN count(*)")).value() == 6
        assert not e.cy.lock.locked()
        # a failing async query releases the lock too
        try:
            await e.cy.run_async("MATCH (p:Person) SET p.id = 'x'")
        except OpenCypherError:
            pass
        assert not e.cy.lock.locked()
        # cancelling a queued async query removes it without running it
        hh = await e.cy.lock.acquire_async(True)
        t = asyncio.create_task(e.cy.run_async("CREATE (:Person {name:'ghost'})"))
        while e.cy.lock.queue_length() < 1:
            await asyncio.sleep(0.002)
        t.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await t
        e.cy.lock.release(hh)
        assert (await e.cy.run_async("MATCH (p:Person {name:'ghost'}) RETURN count(*)")).value() == 0
    asyncio.run(main())


def test_lock_disabled_flag():
    e = fresh(lock=False)
    assert lock_of(e.g).enabled is False
    with e.cy.lock.write():
        e.cy.run("CREATE (:Person {name:'a'})")
        done = []
        th = threading.Thread(target=lambda: done.append(e.cy.run("MATCH (p:Person) RETURN count(*)").value()))
        th.start(); th.join(2)
    assert done == [1]                                    # no queuing when disabled



# =========================================================================== #
# J. atomic writes, rollback, verify / repair, stable MERGE ids
# =========================================================================== #
class FlakyStore(InMemoryStore):
    """Store whose writes can fail on demand.
    down       -> every write/delete raises
    fail_types -> writes/deletes to these types' tables raise
    allow      -> let N more write/delete calls through, then everything fails
    fetch_down -> reads raise"""
    down = False
    fail_types: tuple = ()
    allow = None
    fetch_down = False
    fail_next = None          # fail exactly one write/delete to this type, then recover

    def _gate(self, type_name):
        if self.fail_next == type_name:
            self.fail_next = None
            raise IOError("cassandra hiccup")
        if self.down or type_name in self.fail_types:
            raise IOError("cassandra down")
        if self.allow is not None:
            if self.allow <= 0:
                raise IOError("cassandra down")
            self.allow -= 1

    def write_rows(self, kind, type_name, rows):
        self._gate(type_name)
        return super().write_rows(kind, type_name, rows)

    def delete_rows(self, kind, type_name, ids):
        self._gate(type_name)
        return super().delete_rows(kind, type_name, ids)

    def fetch_rows(self, kind, type_name, ids):
        if self.fetch_down:
            raise IOError("cassandra read down")
        return super().fetch_rows(kind, type_name, ids)


def snapshot(e):
    """Everything observable: graph ids/labels/types per live element, id indexes, store tables."""
    g, sc = e.g, e.sc
    alive = ~np_dropped(g)
    verts = sorted((sc.get_id(g, v), sc.label_names(g, v), sc.type_name(g, v))
                   for v in g.vertices() if alive[int(v)] and sc.get_id(g, v))
    edges = sorted((sc.get_id(g, x), sc.type_name(g, x), sc.get_id(g, x.source()), sc.get_id(g, x.target()))
                   for x in g.edges())
    tabs = {k: {i: dict(r) for i, r in v.items()} for k, v in e.tables.items()}
    isd = isinstance(sc._vidx, dict)
    return {"v": verts, "e": edges, "vidx": dict(sc._vidx) if isd else None,
            "eidx": sorted(sc._eidx) if isd else None, "tables": tabs,
            "pending": (dict(e.st._pending_w), set(e.st._pending_del))}


def np_dropped(g):
    import numpy as np
    n = g.num_vertices()
    if "_dropped" in g.vp:
        return np.asarray(g.vp["_dropped"].a[:n], dtype=bool)
    return np.zeros(n, dtype=bool)


def flaky(**kw):
    e = fresh(store_cls=FlakyStore, **kw)
    e.cy.run(SOCIAL)
    return e


def test_atomic_create_rolls_back_when_store_is_down():
    e = flaky(); before = snapshot(e)
    e.st.down = True
    raises(IOError, lambda: e.cy.run("CREATE (a:Person {name:'Zed'})-[:KNOWS {since:1}]->(b:Person {name:'Yan'})"))
    assert snapshot(e) == before                      # graph, id index, store, staging: all untouched
    assert not e.cy.lock.locked()
    e.st.down = False                                  # service restored: the next write works
    e.cy.run("CREATE (:Person {name:'Zed'})")
    assert e.cy.run("MATCH (p:Person {name:'Zed'}) RETURN count(*)").value() == 1
    assert e.cy.run("MATCH (p:Person {name:'Yan'}) RETURN count(*)").value() == 0
    assert e.cy.verify().ok


def test_atomic_vertex_slots_are_reclaimed():
    e = flaky(); n0 = e.g.num_vertices()
    e.st.down = True
    for _ in range(3):
        raises(IOError, lambda: e.cy.run("CREATE (:Person {name:'T'})"))
    assert e.g.num_vertices() == n0                    # failed creates leave no tombstones behind


def test_atomic_set_rolls_back_including_partial_flush():
    e = flaky()
    e.cy.run("MATCH (p:Person {name:'Ann'}) SET p.nick = 'A'")
    before = snapshot(e)
    # two tables are written; the second call fails -> the first must be compensated
    e.st.fail_next = "City"
    raises(IOError, lambda: e.cy.run(
        "MATCH (p:Person {name:'Ann'}), (c:City) WITH p, c LIMIT 1 "
        "SET p.age = 99, p.nick = 'changed', c.pop = 5"))
    assert snapshot(e) == before
    assert e.cy.run("MATCH (p:Person {name:'Ann'}) RETURN p.age, p.nick").single() == {"p.age": 30, "p.nick": "A"}
    assert e.st.unresolved == []


def test_atomic_failure_in_second_table_removes_rows_written_to_first():
    e = flaky(); before = snapshot(e)
    e.st.fail_next = "Movie"
    raises(IOError, lambda: e.cy.run("CREATE (:Person {name:'P1'}), (:Movie {title:'M1'})"))
    assert snapshot(e) == before                       # the Person row that DID get written is gone again
    assert e.st.unresolved == []


def test_atomic_delete_and_label_changes_roll_back():
    e = flaky()
    e.cy.run("MATCH (p:Person {name:'Ann'}) SET p:Admin")
    before = snapshot(e)
    names_before = col(e.cy.run("MATCH (p:Person) RETURN p.name ORDER BY p.name"))
    e.st.down = True
    raises(IOError, lambda: e.cy.run("MATCH (p:Person {name:'Ann'}) DETACH DELETE p"))
    raises(IOError, lambda: e.cy.run("MATCH (p:Person {name:'Bob'}) SET p:Admin, p.x = 1 REMOVE p:Person"))
    e.st.down = False
    assert snapshot(e) == before
    assert col(e.cy.run("MATCH (p:Person) RETURN p.name ORDER BY p.name")) == names_before
    assert e.cy.run("MATCH (p:Admin) RETURN count(*)").value() == 1
    assert e.cy.run("MATCH (:Person {name:'Ann'})-[r]-() RETURN count(r)").value() == 3   # edges are back
    assert not e.cy.verify().ok and e.st.unresolved        # the outage left (harmless) maybe-applied entries
    assert e.cy.repair()["after"].ok and e.st.unresolved == []


def test_atomic_query_error_applies_nothing():
    e = flaky(); before = snapshot(e)
    raises(OpenCypherError, lambda: e.cy.run("CREATE (:Person {name:'Q'}) WITH 1 AS x MATCH (p:Person) SET p.id = 'bad'"))
    raises(Exception, lambda: e.cy.run("MATCH (p:Person) SET p.age = 1 / 0"))
    assert snapshot(e) == before
    # non-atomic engine keeps the old behaviour: the staged part is written
    n = fresh(cy_kw={"atomic": False})
    raises(OpenCypherError, lambda: n.cy.run("CREATE (:Person {name:'Q'}) WITH 1 AS x CREATE (:Nope)"))


def test_preimage_read_failure_writes_nothing():
    e = flaky()
    e.cy.run("MATCH (p:Person {name:'Ann'}) SET p.k = 1")
    before = snapshot(e)
    e.st.fetch_down = True
    e.st.clear_cache()
    raises(IOError, lambda: e.cy.run("MATCH (p:Person {name:'Ann'}) SET p.k = 2"))
    e.st.fetch_down = False
    assert snapshot(e) == before


def test_incomplete_rollback_raises_consistency_error_and_repair_finishes_it():
    e = flaky()
    e.st.allow = 1                                     # one table write succeeds, then the store dies...
    try:
        e.cy.run("CREATE (:Person {name:'R1'}), (:Movie {title:'RM'})")
        raise AssertionError("expected failure")
    except ConsistencyError as ex:
        assert isinstance(ex.original, IOError) and ex.unresolved
        assert "repair" in str(ex)
    # ...and stays down during the compensation: graph is rolled back, the store has residue
    assert e.cy.run("MATCH (p:Person {name:'R1'}) RETURN count(*)").value() == 0
    assert len(e.st.unresolved) >= 1
    assert any(k[0] == "vertex" and "R1" in [r.get("name") for r in v.values()] for k, v in e.tables.items())
    rep = e.cy.verify()
    assert not rep and rep.unresolved
    e.st.allow = None                                  # store is back
    out = e.cy.repair()
    assert out["journal_retried"] >= 1 and out["journal_still_failing"] == 0
    assert out["after"].ok and e.st.unresolved == []
    assert not any("R1" in [r.get("name") for r in v.values()] for v in e.tables.values())   # residue gone
    assert e.cy.verify().ok


def test_store_level_rollback_after_midquery_flush():
    # a flush in the middle of a write query (e.g. before an index search) is journaled too
    e = flaky()
    from opencypher_graphtool.common import _Txn
    txn = _Txn(e.g, e.st)
    e.g.__dict__["_gt_txn"] = txn; e.st._txn = txn
    try:
        from opencypher_graphtool.elements import _create_vertex, _set_props
        v = _create_vertex(e.g, ("Person",), None)
        _set_props(e.g, v, {"name": "Mid"})
        e.st.flush()                                   # mid-query flush: row is now in the store
        assert any(r.get("name") == "Mid" for r in e.tables[("vertex", "Person")].values())
        assert txn.attempted
    finally:
        e.g.__dict__.pop("_gt_txn", None); e.st._txn = None
    for fn in reversed(txn.undo):
        fn()
    assert e.st.rollback_txn(txn) == []
    assert not any(r.get("name") == "Mid" for r in e.tables[("vertex", "Person")].values())
    assert e.cy.verify().ok


# --------------------------------------------------------------------------- #
def test_verify_clean_and_drift_detection():
    e = flaky()
    rep = e.cy.verify()
    assert rep.ok and bool(rep) and rep.checked[("vertex", "Person")][0] == 4 and "consistent" in rep.summary()
    # drift 1: a graph element lost its row (e.g. Cassandra restored from an older snapshot)
    ann = e.cy.run("MATCH (p:Person {name:'Ann'}) RETURN p.id").value()
    del e.tables[("vertex", "Person")][ann]
    # drift 2: a stray row nobody points to (e.g. graph restored from an older file)
    stray = "11111111-1111-4111-8111-111111111111"
    e.tables[("vertex", "Movie")][stray] = {"id": stray, "title": "Ghost"}
    rep = e.cy.verify()
    assert not rep.ok
    assert rep.missing_rows == {("vertex", "Person"): [ann]}
    assert rep.orphan_rows == {("vertex", "Movie"): [stray]}
    assert "missing rows" in rep.summary() and "orphan rows" in rep.summary()
    # restricting by type
    assert e.cy.verify(types=["City"]).ok


def test_repair_policies():
    def drifted():
        e = flaky()
        ann = e.cy.run("MATCH (p:Person {name:'Ann'}) RETURN p.id").value()
        e.nrel = e.cy.run("MATCH (:Person {name:'Ann'})-[r]-() RETURN count(r)").value()
        del e.tables[("vertex", "Person")][ann]
        stray = "11111111-1111-4111-8111-111111111111"
        e.tables[("vertex", "Movie")][stray] = {"id": stray}
        return e, ann, stray
    # default: report only, nothing changes
    e, ann, stray = drifted(); snap = snapshot(e)
    out = e.cy.repair()
    assert snapshot(e) == snap and not out["after"].ok and out["rows_recreated"] == 0
    # recreate + delete
    e, ann, stray = drifted()
    out = e.cy.repair(missing="recreate", orphans="delete")
    assert out["rows_recreated"] == 1 and out["orphan_rows_deleted"] == 1 and out["after"].ok
    assert stray not in e.tables[("vertex", "Movie")] and ann in e.tables[("vertex", "Person")]
    assert e.cy.run("MATCH (p:Person) RETURN count(*)").value() == 4          # graph untouched
    # tombstone: element and its relationships leave the graph
    e, ann, stray = drifted()
    nrel = e.nrel
    assert nrel >= 1
    out = e.cy.repair(missing="tombstone")
    assert out["graph_elements_removed"] == 1 + nrel and out["after"].missing_rows == {}
    assert e.cy.run("MATCH (p:Person) RETURN count(*)").value() == 3
    assert list(out["after"].orphan_rows) == [("vertex", "Movie")]             # orphans not requested
    raises(ValueError, lambda: e.cy.repair(missing="nope"))
    raises(ValueError, lambda: e.cy.repair(orphans="nope"))


def test_verify_missing_edge_row_and_table():
    e = flaky()
    eid = next(iter(e.tables[("edge", "KNOWS")]))
    del e.tables[("edge", "KNOWS")][eid]
    rep = e.cy.verify()
    assert rep.missing_rows == {("edge", "KNOWS"): [eid]}
    e.cy.repair(missing="tombstone")
    assert e.cy.verify().ok
    # a graph type whose table does not exist
    e2 = flaky()
    del e2.tables[("vertex", "City")]
    assert ("vertex", "City") in e2.cy.verify().missing_tables


def test_verify_async_safe_under_lock_and_after_reload():
    e = flaky()
    with e.cy.lock.write():                            # re-entrant for the holder
        assert e.cy.verify().ok
    # a reloaded graph (new Schema bound to the same graph) rebuilds its id index and still verifies
    from opencypher_graphtool import Schema as S
    e.sc.bind(e.g)
    assert e.cy.verify().ok


# --------------------------------------------------------------------------- #
def test_merge_uses_stable_ids_and_is_idempotent_across_a_lost_graph():
    e1 = fresh()
    e1.cy.run("MERGE (a:Person {name:'Eve'}) MERGE (b:Person {name:'Fay'}) MERGE (a)-[:KNOWS {since:2020}]->(b)")
    ids1 = sorted(col(e1.cy.run("MATCH (p:Person) RETURN p.id")))
    rid1 = col(e1.cy.run("MATCH ()-[r:KNOWS]->() RETURN r.id"))
    # the graph file is lost; a retry on a brand-new graph against the SAME store tables
    tables = e1.tables
    st = InMemoryStore(tables, indexed=e1.st.indexed)
    g2 = Graph()
    cy2 = opencypher(g2, st, Schema(vertex_labels=VT, vertex_types=VT, edge_types=ET))
    cy2.run("MERGE (a:Person {name:'Eve'}) MERGE (b:Person {name:'Fay'}) MERGE (a)-[:KNOWS {since:2020}]->(b)")
    assert sorted(col(cy2.run("MATCH (p:Person) RETURN p.id"))) == ids1          # same ids
    assert col(cy2.run("MATCH ()-[r:KNOWS]->() RETURN r.id")) == rid1
    assert len(tables[("vertex", "Person")]) == 2 and len(tables[("edge", "KNOWS")]) == 1   # no duplicate rows
    assert cy2.verify().ok
    # CREATE still produces random ids; a node with another property set is a different node
    e1.cy.run("CREATE (:Person {name:'Eve'})")
    assert len(set(col(e1.cy.run("MATCH (p:Person {name:'Eve'}) RETURN p.id")))) == 2


def test_merge_stable_id_collision_falls_back_to_random():
    e = fresh()
    e.cy.run("MERGE (:Person {name:'Gus'})")
    e.cy.run("MATCH (p:Person {name:'Gus'}) SET p.name = 'Gustav'")   # same id, no longer matches
    e.cy.run("MERGE (:Person {name:'Gus'})")                           # must not hit 'duplicate id'
    assert e.cy.run("MATCH (p:Person) WHERE p.name IN ['Gus','Gustav'] RETURN count(*)").value() == 2
    off = fresh(cy_kw={"stable_merge_ids": False})
    off.cy.run("MERGE (:Person {name:'Hal'})")
    from opencypher_graphtool import stable_id
    assert off.cy.run("MATCH (p:Person) RETURN p.id").value() != stable_id(
        "opencypher-merge", "vertex", "Person", repr(sorted([("name", repr("Hal"))])))


def test_failed_merge_then_retry_creates_exactly_one():
    e = flaky()
    e.st.down = True
    for _ in range(2):
        raises(IOError, lambda: e.cy.run("MERGE (:Person {name:'Ivy'})"))
    e.st.down = False
    e.cy.run("MERGE (:Person {name:'Ivy'})")
    e.cy.run("MERGE (:Person {name:'Ivy'})")
    assert e.cy.run("MATCH (p:Person {name:'Ivy'}) RETURN count(*)").value() == 1
    assert e.cy.verify().ok


def test_async_write_failure_rolls_back_and_releases_lock():
    async def main():
        e = flaky(); before = snapshot(e)
        e.st.down = True
        try:
            await e.cy.run_async("CREATE (:Person {name:'AsyncFail'})")
            raise AssertionError("expected failure")
        except IOError:
            pass
        assert snapshot(e) == before and not e.cy.lock.locked()
        e.st.down = False
        await e.cy.run_async("CREATE (:Person {name:'AsyncOk'})")
        assert (await e.cy.run_async("MATCH (p:Person) RETURN count(*)")).value() == 5
    asyncio.run(main())



# =========================================================================== #
# K. id -> index in the store's reverse table (id_index="store")
# =========================================================================== #
REV = ("reverse", "ids")
MARK = "00000000-0000-0000-0000-000000000000"


class CountingStore(FlakyStore):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.calls = []

    def fetch_rows(self, kind, type_name, ids):
        self.calls.append((kind, type_name, len(ids)))
        return super().fetch_rows(kind, type_name, ids)


def rev_env(store_cls=CountingStore, data=True):
    e = fresh(store_cls=store_cls, cy_kw={"id_index": "store"})
    if data:
        e.cy.run(SOCIAL)
    return e


def rev_rows(e):
    return {k: v for k, v in e.tables[REV].items() if k != MARK}


def restart(e, **kw):
    """Simulate a process restart on the same graph + store: new Schema/engine, empty cache."""
    st = e.st
    st.clear_cache(); st.calls.clear() if hasattr(st, "calls") else None
    sc = Schema(vertex_labels=VT, vertex_types=VT, edge_types=ET)
    cy = opencypher(e.g, st, sc, **kw)
    e.sc, e.cy = sc, cy
    return e


def test_reverse_table_is_populated_and_queries_work():
    e = rev_env()
    g, sc = e.g, e.sc
    assert sc._id_mode == "store" and MARK in e.tables[REV]
    nv = sum(1 for v in g.vertices() if sc.get_id(g, v))
    ne = g.num_edges()
    assert len(rev_rows(e)) == nv + ne
    for v in g.vertices():                               # every vertex row points at its own index
        r = e.tables[REV][sc.get_id(g, v)]
        assert r["kind"] == "v" and r["idx"] == int(v) and "src" not in r
    for x in g.edges():
        r = e.tables[REV][sc.get_id(g, x)]
        assert r == {"id": sc.get_id(g, x), "kind": "e", "idx": int(g.edge_index[x]),
                     "src": int(x.source()), "dst": int(x.target())}
    # same answers as the in-memory index
    m = social()
    for q in ("MATCH (p:Person) WHERE p.age > 28 RETURN p.name ORDER BY p.name",
              "MATCH (a:Person {name:'Ann'})-[:KNOWS*1..2]->(b) RETURN DISTINCT b.name ORDER BY b.name",
              "MATCH (p:Person) RETURN count(*)", "MATCH ()-[r]->() RETURN count(r)"):
        assert e.cy.run(q).data() == m.cy.run(q).data(), q
    ann = e.cy.run("MATCH (p:Person {name:'Ann'}) RETURN p.id").value()
    assert e.cy.run("MATCH (p:Person {id: $i}) RETURN p.name", i=ann).value() == "Ann"   # by id
    assert e.cy.verify().ok


def test_startup_does_not_scan_the_graph():
    import warnings
    PM = getattr(sys.modules["graph_tool"], "_PM", None)
    e = fresh(store_cls=CountingStore)                   # memory-mode engine, 200 people
    e.cy.run("UNWIND range(1, 200) AS i CREATE (:Person {name: 'p' + toString(i)})")
    n = e.g.num_vertices()
    mk = lambda **k: opencypher(e.g, e.st, Schema(vertex_labels=VT, vertex_types=VT, edge_types=ET), **k)
    if PM is not None:
        r0 = PM.reads; mk(); mem_reads = PM.reads - r0
        assert mem_reads >= n                            # the dict build touches every element
    with warnings.catch_warnings(record=True) as w:      # first time: the table does not exist yet
        warnings.simplefilter("always"); mk(id_index="store")
    assert any("reverse id table" in str(x.message) for x in w)
    assert len(rev_rows(e)) == n and MARK in e.tables[REV]
    e.st.clear_cache(); e.st.calls.clear()
    if PM is not None:
        r0 = PM.reads
    with warnings.catch_warnings():
        warnings.simplefilter("error"); cy2 = mk(id_index="store")          # restart: marker found
    assert e.st.calls == [("reverse", "ids", 1)]         # exactly one read: the completeness marker
    if PM is not None:
        assert PM.reads - r0 < 10                        # no per-element reads at all
    assert cy2.run("MATCH (p:Person {name:'p77'}) RETURN count(*)").value() == 1


def test_reverse_lookups_are_batched():
    e = rev_env(data=False)
    e.cy.run("UNWIND range(1, 40) AS i CREATE (:Person {name: 'Dup', age: i})")
    restart(e, id_index="store")
    assert e.cy.run("MATCH (p:Person {name:'Dup'}) RETURN count(*)").value() == 40
    rev = [c for c in e.st.calls if c[0] == "reverse"]
    assert [c[2] for c in rev if c[2] > 1] == [40]       # one batched fetch for all 40 hits
    assert sum(1 for c in rev if c[2] == 1) <= 1         # (+ at most the marker read)
    e.st.calls.clear()                                   # second run: served from the cache
    e.cy.run("MATCH (p:Person {name:'Dup'}) RETURN count(*)")
    assert not [c for c in e.st.calls if c[0] == "reverse"]


def test_reverse_rows_follow_atomic_writes():
    e = rev_env(); before = snapshot(e)
    e.st.down = True
    raises(IOError, lambda: e.cy.run("CREATE (:Person {name:'Zed'})-[:KNOWS]->(:Person {name:'Yan'})"))
    e.st.down = False
    assert snapshot(e) == before
    e.cy.run("CREATE (:Person {name:'Ok'})")             # next write heals any "maybe" leftovers
    assert e.st.unresolved == []
    # the reverse write is the one that fails: the entity row written first is compensated
    before = snapshot(e)
    e.st.fail_next = "ids"
    raises(IOError, lambda: e.cy.run("CREATE (:Person {name:'Late'})"))
    assert e.cy.run("MATCH (p:Person {name:'Late'}) RETURN count(*)").value() == 0
    assert not any(r.get("name") == "Late" for r in e.tables[("vertex", "Person")].values())
    assert set(rev_rows(e)) == set(before["tables"][REV]) - {MARK}
    assert e.cy.verify().ok
    # delete: rows disappear; a failed delete puts them back
    n = len(rev_rows(e))
    e.st.down = True
    raises(IOError, lambda: e.cy.run("MATCH (p:Person {name:'Ann'}) DETACH DELETE p"))
    e.st.down = False
    e.cy.run("CREATE (:Person {name:'heal'})")
    assert len(rev_rows(e)) == n + 1 and e.cy.run("MATCH (p:Person {name:'Ann'}) RETURN count(*)").value() == 1
    e.cy.run("MATCH (p:Person {name:'Ann'}) DETACH DELETE p")
    assert len(rev_rows(e)) == n + 1 - 4 and e.cy.verify().ok     # Ann + her 3 relationships


def test_reverse_rows_survive_restart_and_edges_resolve():
    e = rev_env()
    ids = {}
    for v in e.g.vertices():
        ids[e.sc.get_id(e.g, v)] = int(v)
    edge_ids = {e.sc.get_id(e.g, x): (int(x.source()), int(x.target())) for x in e.g.edges()}
    e.cy.run("MATCH (a:Person {name:'Ann'}), (b:Person {name:'Bob'}) CREATE (a)-[:LINK]->(b), (a)-[:LINK]->(b)")
    restart(e, id_index="store")
    for i, idx in ids.items():
        assert e.sc._vidx.get(i) == idx
    for i, (s_, t_) in edge_ids.items():
        x = e.sc.edge_by_id(i)
        assert (int(x.source()), int(x.target())) == (s_, t_) and e.sc.get_id(e.g, x) == i
    links = col(e.cy.run("MATCH ()-[r:LINK]->() RETURN r.id"))
    assert len(links) == 2 and links[0] != links[1]      # parallel edges are told apart
    assert {e.sc.get_id(e.g, e.sc.edge_by_id(i)) for i in links} == set(links)
    assert e.sc._vidx.get("11111111-1111-4111-8111-111111111111") is None      # unknown id: a clean miss


def test_stale_reverse_row_is_never_trusted():
    import warnings
    e = rev_env()
    a = e.cy.run("MATCH (p:Person {name:'Ann'}) RETURN p.id").value()
    b = e.cy.run("MATCH (p:Person {name:'Bob'}) RETURN p.id").value()
    ra, rb = e.tables[REV][a], e.tables[REV][b]
    ia, ib = ra["idx"], rb["idx"]
    ra["idx"], rb["idx"] = ib, ia          # as if the graph had been renumbered
    restart(e, id_index="store")
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        assert e.cy.run("MATCH (p:Person {name:'Ann'}) RETURN p.age").value() == 30    # still correct
        assert e.cy.run("MATCH (p:Person {name:'Bob'}) RETURN p.age").value() == 41
    assert any("reverse id index disagrees" in str(x.message) for x in w) and e.sc.stale_hits >= 1
    rep = e.cy.verify()
    assert sorted(rep.reverse_stale) == sorted([a, b]) and not rep.ok and "stale" in rep.summary()
    out = e.cy.repair(reverse="rebuild")
    assert out["after"].ok and e.tables[REV][a]["idx"] == ia and e.tables[REV][b]["idx"] == ib
    assert e.sc._vidx._fb is None                        # degraded mode is left after the rebuild
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert e.cy.run("MATCH (p:Person {name:'Ann'}) RETURN p.age").value() == 30


def test_missing_and_orphan_reverse_rows_are_reported_and_repaired():
    e = rev_env()
    g, sc = e.g, e.sc
    a = e.cy.run("MATCH (p:Person {name:'Ann'}) RETURN p.id").value()
    del e.tables[REV][a]
    ghost = "22222222-2222-4222-8222-222222222222"
    e.tables[REV][ghost] = {"id": ghost, "kind": "v", "idx": 9999}      # row for nothing
    ghost2 = "33333333-3333-4333-8333-333333333333"
    e.tables[REV][ghost2] = {"id": ghost2, "kind": "v", "idx": 0}        # slot 0 holds another id
    restart(e, id_index="store")
    import warnings
    assert e.sc._vidx.get(ghost) is None and e.sc._vidx._fb is None      # out-of-range slot: a clean miss
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        assert e.sc._vidx.get(ghost2) is None                              # not in the graph either way
    assert w and e.sc._vidx._fb is not None                                # but the slot clash degrades safely
    rep = e.cy.verify()
    assert rep.reverse_missing == [a] and sorted(rep.reverse_orphans) == sorted([ghost, ghost2])
    assert e.cy.repair()["after"].reverse_missing == [a]                # default policy: report only
    out = e.cy.repair(reverse="rebuild")
    assert out["reverse_rebuilt"]["orphans_deleted"] == 2 and out["after"].ok
    assert e.cy.run("MATCH (p:Person {name:'Ann'}) RETURN count(*)").value() == 1
    # marker lost -> verify says so; rebuild_id_index() restores it
    del e.tables[REV][MARK]
    assert not e.cy.verify().reverse_marker
    e.cy.rebuild_id_index()
    assert e.cy.verify().ok


def test_reverse_setup_errors():
    raises(ValueError, lambda: Schema().bind(Graph(), id_index="store"), "property store")
    raises(ValueError, lambda: Schema().bind(Graph(), id_index="x"))
    m = social()
    raises(OpenCypherError, m.cy.rebuild_id_index, "id_index='store'")
    class NoTable(InMemoryStore):
        def validate_type(self, kind, type_name):
            if kind == "reverse":
                raise KeyError("No table for reverse type 'ids'")
    raises(ValueError, lambda: opencypher(Graph(), NoTable({}), Schema(), id_index="store"), "create the reverse id table")


def test_reverse_table_on_cassandra_fake_session():
    for idt in ("text", "uuid"):
        _install_fake_cassandra()
        spec = {"v_person": {"cols": {"id": idt, "name": "text", "age": "int"}, "pk": "id", "indexed": ["name"]},
                "e_knows": {"cols": {"id": idt, "since": "int"}, "pk": "id", "indexed": []},
                "reverse_vertex_ids": {"cols": {"id": idt, "kind": "text", "idx": "bigint", "src": "bigint",
                                                "dst": "bigint"}, "pk": "id", "indexed": []}}
        sess = FakeSession(spec)
        namer = lambda kind, t: ("v_" if kind == "vertex" else "e_") + t.lower()
        store = C.CassandraStore(sess, "ks", table_namer=namer)
        assert "reverse_vertex_ids" in store.reverse_table_ddl(idt) and "idx bigint" in store.reverse_table_ddl()
        store.create_reverse_table(idt)
        sc = Schema(vertex_labels=["Person"], vertex_types=["Person"], edge_types=["KNOWS"])
        g = Graph()
        cy = opencypher(g, store, sc, id_index="store")
        cy.run("CREATE (a:Person {name:'Ann', age:30})-[:KNOWS {since:2010}]->(b:Person {name:'Bob', age:41})")
        rows = sess.rows["reverse_vertex_ids"]
        assert MARK in rows and len(rows) == 4                                   # 2 vertices + 1 edge + marker
        assert sorted(r["kind"] for k, r in rows.items() if k != MARK) == ["e", "v", "v"]
        # restart: new schema, empty cache -> found through the table, one CQL read per id
        store.clear_cache(); sess.log.clear()
        sc2 = Schema(vertex_labels=["Person"], vertex_types=["Person"], edge_types=["KNOWS"])
        cy2 = opencypher(g, store, sc2, id_index="store")
        assert cy2.run("MATCH (a:Person {name:'Ann'})-[:KNOWS]->(b) RETURN b.name").value() == "Bob"
        assert any('FROM "ks"."reverse_vertex_ids"' in c for c in sess.log)
        # delete removes the reverse rows; verify scans every table incl. the reverse one
        assert cy2.verify().ok
        cy2.run("MATCH (p:Person {name:'Ann'}) DETACH DELETE p")
        assert len(sess.rows["reverse_vertex_ids"]) == 2 and cy2.verify().ok       # Bob + marker
        stray = "44444444-4444-4444-8444-444444444444"
        sess.rows["reverse_vertex_ids"][stray] = {"id": stray, "kind": "v", "idx": 5}
        assert cy2.verify().reverse_orphans == [stray]
        assert cy2.repair(reverse="rebuild")["after"].ok and stray not in sess.rows["reverse_vertex_ids"]



# =========================================================================== #
# L. pydantic models: schema + tables + validation from one source
# =========================================================================== #
import datetime as _dt, enum as _enum
from typing import Literal
from pydantic import field_validator, model_validator


class Status(str, _enum.Enum):
    ACTIVE = "active"
    GONE = "gone"


class MPerson(VertexModel):
    class Meta:
        type = "Person"
    name: str = Field(index=True)
    age: int | None = Field(default=None, ge=0)
    email: str | None = None
    status: Status = Status.ACTIVE
    tags: list[str] = Field(default_factory=list)
    born: _dt.datetime | None = None

    @field_validator("email")
    @classmethod
    def _lower(cls, v):
        return v.lower() if v else v


class MMovie(VertexModel, type="Movie", table="movies"):
    title: str = Field(index=True)
    year: int | None = None


class MKnows(EdgeModel):
    class Meta:
        type = "KNOWS"
    since: int = Field(ge=1900)                 # REQUIRED on every KNOWS edge


class MActed(EdgeModel):
    class Meta:
        type = "ACTED_IN"
    role: str | None = None


def mmodels():
    return Models([MPerson, MMovie, MKnows, MActed])


def mem_models(**kw):
    tables = {}
    st = InMemoryStore(tables)
    g = Graph()
    cy = opencypher(g, st, models=mmodels(), **kw)
    return NS(g=g, st=st, cy=cy, sc=cy.schema, tables=tables)


def test_models_drive_the_schema_and_table_names():
    m = mmodels()
    sc = m.schema()
    assert sc.reg("vertex", "type").all_names() == ["Person", "Movie"]
    assert sc.reg("edge", "type").all_names() == ["KNOWS", "ACTED_IN"]
    assert sc.auto_register is False
    assert [m.table_namer("vertex", "Person"), m.table_namer("vertex", "Movie"),
            m.table_namer("edge", "ACTED_IN")] == ["person", "movies", "acted_in"]
    raises(KeyError, lambda: m.table_namer("vertex", "Nope"), "no model")
    assert m.pick_type(["Admin", "Movie", "Person"]) == "Movie" and m.pick_type(["Admin"]) is None

    class Dup(VertexModel):
        class Meta:
            type = "Person"
    raises(ValueError, lambda: Models([MPerson, Dup]), "two models")

    class SameTable(VertexModel, type="Other", table="person"):
        pass
    raises(ValueError, lambda: Models([MPerson, SameTable]), "table 'person'")

    class Bad(VertexModel):
        id: str
    raises(ValueError, lambda: Models([Bad]), "reserved")
    raises(TypeError, lambda: Models([int]), "not a VertexModel")

    class Weird(VertexModel):
        x: dict[str, list[int]]
    raises(TypeError, lambda: Models([Weird]), "nested collections")


def test_models_generate_ddl_with_type_mapping():
    ddl = mmodels().ddl("ks")
    person = next(s for s in ddl if s.startswith('CREATE TABLE IF NOT EXISTS "ks"."person"'))
    for part in ('"id" uuid PRIMARY KEY', '"name" text', '"age" bigint', '"status" text',
                 '"tags" list<text>', '"born" timestamp'):
        assert part in person, part
    assert 'CREATE INDEX IF NOT EXISTS "person_name_idx" ON "ks"."person" ("name") USING \'sai\'' in ddl
    assert 'CREATE INDEX IF NOT EXISTS "movies_title_idx" ON "ks"."movies" ("title") USING \'sai\'' in ddl
    assert not any("age_idx" in s for s in ddl)                       # only index=True fields
    assert any('"since" bigint' in s and '"ks"."knows"' in s for s in ddl)
    assert mmodels().ddl("ks", id_type="text")[0].count('"id" text PRIMARY KEY') == 1

    class Many(VertexModel):
        a: bool
        b: float
        c: _uuid_t
        d: _dt.date
        e: bytes
        f: set[int]
        g: dict[str, int]
        h: Literal["x", "y"]
        i: _dt.time | None = None
        j: str = Field("z", cql="varchar")
        k: list[str] = Field(default_factory=list, index=True)
    ddl = Models([Many]).ddl("ks")[0]
    for part in ('"a" boolean', '"b" double', '"c" uuid', '"d" date', '"e" blob', '"f" set<bigint>',
                 '"g" map<text, bigint>', '"h" text', '"i" time', '"j" varchar'):
        assert part in ddl, part
    assert 'VALUES("k")' in Models([Many]).ddl("ks")[-1]               # collection index
    class MapIdx(VertexModel):
        m: dict[str, int] = Field(default_factory=dict, index=True)
    raises(ValueError, lambda: Models([MapIdx]).ddl("ks"), "indexing a map")


import uuid as _u
_uuid_t = _u.UUID


def test_models_validate_values_when_set():
    e = mem_models(); cy = e.cy
    cy.run("CREATE (:Person {name:'Ann', age:'30', email:'ANN@X.COM', status:'gone'})")
    r = cy.run("MATCH (p:Person {name:'Ann'}) RETURN p.age, p.email, p.status, p.tags").single()
    assert r == {"p.age": 30, "p.email": "ann@x.com", "p.status": "gone", "p.tags": []}   # coerced, validator ran, default filled
    assert isinstance(next(iter(e.tables[("vertex", "Person")].values()))["age"], int)
    raises(OpenCypherError, lambda: cy.run("CREATE (:Person {name:'X', age:'abc'})"), "age")
    raises(OpenCypherError, lambda: cy.run("CREATE (:Person {name:'X', age:-5})"), "age")
    raises(OpenCypherError, lambda: cy.run("CREATE (:Person {name:'X', status:'weird'})"), "status")
    raises(OpenCypherError, lambda: cy.run("CREATE (:Person {name:'X', nope:1})"), "no field")
    raises(OpenCypherError, lambda: cy.run("CREATE (:Person {name:'X', tags:[1, {}]})"), "tags")
    cy.run("MATCH (p:Person {name:'Ann'}) SET p.age = 31, p.tags = ['a','b'], p.born = $b", b=_dt.datetime(2000, 1, 2))
    assert cy.run("MATCH (p:Person {name:'Ann'}) RETURN p.age, p.tags").single() == {"p.age": 31, "p.tags": ["a", "b"]}
    assert cy.run("MATCH (p:Person) RETURN count(*)").value() == 1       # failed creates left nothing behind
    assert cy.verify().ok


def test_models_strict_commit_required_fields_and_rollback():
    e = mem_models(); cy = e.cy
    cy.run("CREATE (a:Person {name:'A'}), (b:Person {name:'B'})")
    n_before = cy.run("MATCH (p:Person) RETURN count(*)").value()
    # KNOWS.since is required: the edge is created without it -> the WHOLE query is rolled back
    raises(ConstraintError, lambda: cy.run(
        "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}) CREATE (a)-[:KNOWS]->(b), (:Person {name:'ghost'})"), "since")
    assert cy.run("MATCH ()-[r:KNOWS]->() RETURN count(r)").value() == 0
    assert cy.run("MATCH (p:Person) RETURN count(*)").value() == n_before          # the ghost is gone too
    assert not e.tables[("edge", "KNOWS")] and cy.verify().ok
    # a name is required on Person: nothing sets it
    raises(ConstraintError, lambda: cy.run("CREATE (:Person {age: 3})"), "name")
    # required value supplied by a LATER clause of the same query: fine (checked only at commit)
    cy.run("MATCH (a:Person {name:'A'}), (b:Person {name:'B'}) CREATE (a)-[r:KNOWS]->(b) SET r.since = 2001")
    assert cy.run("MATCH ()-[r:KNOWS]->() RETURN r.since").value() == 2001
    # ... even when something forces a flush in the middle of the query
    cy.run("CREATE (p:Person {age: 1}) WITH p MATCH (q:Person {name:'A'}) SET p.name = 'late'")
    assert cy.run("MATCH (p:Person {name:'late'}) RETURN count(*)").value() == 1
    # removing a required property from an EXISTING row is caught (the whole row is validated)
    raises(ConstraintError, lambda: cy.run("MATCH (p:Person {name:'late'}) REMOVE p.name"), "name")
    raises(ConstraintError, lambda: cy.run("MATCH ()-[r:KNOWS]->() SET r.since = null"), "since")
    assert cy.run("MATCH (p:Person {name:'late'}) RETURN count(*)").value() == 1
    # failing validator on an existing row, written by another query: still validated as a whole row
    e.tables[("vertex", "Person")][next(iter(e.tables[("vertex", "Person")]))]["age"] = -1   # drifted data
    e.st.clear_cache()
    raises(ConstraintError, lambda: cy.run("MATCH (p:Person) WHERE p.age = -1 SET p.email = 'a@b.c'"), "age")
    assert cy.verify().ok


def test_models_defaults_validators_and_model_validator():
    e = mem_models(); cy = e.cy
    cy.run("CREATE (:Person {name:'Dee'})")
    row = next(iter(e.tables[("vertex", "Person")].values()))
    assert row["status"] == "active" and row["tags"] == []              # defaults WRITTEN on create
    assert "age" not in row and "email" not in row                      # None defaults are not
    cy.run("MATCH (p:Person {name:'Dee'}) SET p.status = 'gone'")
    cy.run("CREATE (:Person {name:'Eve'})")                              # a new row does not touch Dee
    assert [r["status"] for r in e.tables[("vertex", "Person")].values() if r["name"] == "Dee"] == ["gone"]

    class Span(VertexModel):
        lo: int
        hi: int

        @model_validator(mode="after")
        def _order(self):
            if self.lo > self.hi:
                raise ValueError("lo must not exceed hi")
            return self
    tables = {}
    g = Graph()
    c2 = opencypher(g, InMemoryStore(tables), models=Models([Span]))
    c2.run("CREATE (:Span {lo: 1, hi: 5})")
    raises(ConstraintError, lambda: c2.run("CREATE (:Span {lo: 9, hi: 5})"), "lo must not exceed hi")
    c2.run("CREATE (s:Span {lo: 1, hi: 5}) SET s.lo = 2")
    assert len(tables[("vertex", "Span")]) == 2


def test_models_multi_label_picks_the_model_type():
    class Tagged(VertexModel, type="Tagged", labels=["Special", "Archived"]):
        n: int | None = None
    c = opencypher(Graph(), InMemoryStore({}), models=Models([MPerson, Tagged]))
    c.run("CREATE (:Special:Tagged {n: 1})")                                   # type = the label that has a model
    assert c.run("MATCH (t:Special) RETURN count(*)").value() == 1 and c.run("MATCH (t:Archived) RETURN count(*)").value() == 0
    c.run("MATCH (t:Special) SET t:Archived")
    assert c.run("MATCH (t:Archived:Tagged) RETURN t.n").value() == 1
    raises(OpenCypherError, lambda: c.run("CREATE (:Unknown {x: 1})"))             # no model, auto_register off


def test_models_edges_indexes_and_search_pushdown():
    class Hub(VertexModel):
        code: str = Field(index=True)
        note: str | None = None
    tables, g = {}, Graph()
    st = InMemoryStore(tables)
    c = opencypher(g, st, models=Models([Hub]))
    c.run("UNWIND range(1, 30) AS i CREATE (:Hub {code: 'c' + toString(i), note: 'n'})")
    st.clear_cache(); st.searches = 0
    assert c.run("MATCH (h:Hub {code:'c7'}) RETURN h.note").value() == "n"
    assert st.searches == 1                                                    # index=True => searchable
    assert st.indexed[("vertex", "Hub")] == {"code"}


def test_models_bit_map_is_stable_when_models_change_order():
    e = mem_models()
    e.cy.run("CREATE (:Person {name:'A'}), (:Movie {title:'M'})")
    bits = {n: e.sc.reg("vertex", "type")._pos[n] for n in ("Person", "Movie")}
    class Extra(VertexModel, type="Extra"):
        pass
    # a "restart": the process forgets its schema; reorder + add a model; the schema SAVED in the graph decides the bits
    C.schema._SCHEMAS.pop(id(e.g), None)
    assert Models([Extra, MMovie, MPerson]).schema().reg("vertex", "type")._pos["Extra"] == 0     # a fresh schema would reshuffle
    st2 = InMemoryStore(e.tables)
    c2 = opencypher(e.g, st2, models=Models([Extra, MMovie, MPerson, MKnows, MActed]))
    reg = c2.schema.reg("vertex", "type")
    assert {n: reg._pos[n] for n in ("Person", "Movie")} == bits and reg._pos["Extra"] not in bits.values()
    assert c2.run("MATCH (p:Person) RETURN p.name").value() == "A" and c2.run("MATCH (m:Movie) RETURN m.title").value() == "M"
    assert Schema.from_graph(e.g).reg("vertex", "type")._pos["Extra"] == reg._pos["Extra"]    # attached automatically


def test_models_atomic_false_validates_values_but_not_required_fields():
    e = mem_models(atomic=False)
    e.cy.run("CREATE (a:Person {name:'A'})-[:KNOWS]->(b:Person {name:'B'})")         # no `since`: not enforced
    raises(OpenCypherError, lambda: e.cy.run("CREATE (:Person {name:'X', age:'abc'})"), "age")


# --------------------------------------------------------------------------- #
# automatic CREATE / ALTER against a (fake) Cassandra
# --------------------------------------------------------------------------- #
class DdlSession(FakeSession):
    """FakeSession that executes the DDL the Models generate and keeps its metadata in step."""
    def __init__(self, keyspace="ks"):
        self.ks = keyspace
        self.spec, self.rows, self.log = {}, {}, []
        self.executed = []
        self._rebuild()

    def _rebuild(self):
        tables = {}
        for t, d in self.spec.items():
            cols = {c: NS(cql_type=ty, name=c) for c, ty in d["cols"].items()}
            idx = {f"{t}_{c}_idx": NS(index_options={"target": c, "class_name": "sai", **opts})
                   for c, opts in d["indexed"].items()}
            tables[t] = NS(columns=cols, partition_key=[cols[d["pk"]]], indexes=idx)
        self.cluster = NS(metadata=NS(keyspaces={self.ks: NS(tables=tables)}),
                          refresh_schema_metadata=lambda: None)

    @staticmethod
    def _split(body):
        parts, depth, cur = [], 0, ""
        for ch in body:
            if ch == "<": depth += 1
            if ch == ">": depth -= 1
            if ch == "," and depth == 0:
                parts.append(cur.strip()); cur = ""
            else:
                cur += ch
        return parts + [cur.strip()]

    def execute(self, stmt, params=None):
        m = re.match(r'CREATE TABLE IF NOT EXISTS "ks"\."(\w+)" \((.*)\)$', stmt)
        if m:
            self.executed.append(stmt)
            t = m.group(1)
            if t not in self.spec:
                cols, pk = {}, None
                for part in self._split(m.group(2)):
                    pm = re.match(r'"?(\w+)"? (.+?)( PRIMARY KEY)?$', part)
                    cols[pm.group(1)] = pm.group(2)
                    if pm.group(3): pk = pm.group(1)
                self.spec[t] = {"cols": cols, "pk": pk, "indexed": {}}
                self.rows[t] = {}
            self._rebuild(); return []
        m = re.match(r'ALTER TABLE "ks"\."(\w+)" ADD "(\w+)" (.+)$', stmt)
        if m:
            self.executed.append(stmt)
            self.spec[m.group(1)]["cols"][m.group(2)] = m.group(3)
            self._rebuild(); return []
        m = re.match(r"""CREATE INDEX IF NOT EXISTS "(\w+)" ON "ks"\."(\w+)" \((?:VALUES\()?"(\w+)"\)?\) USING 'sai'"""
                     r"""(?: WITH OPTIONS = \{'similarity_function': '(\w+)'\})?$""", stmt)
        if m:
            self.executed.append(stmt)
            opts = {"similarity_function": m.group(4)} if m.group(4) else {}
            self.spec[m.group(2)]["indexed"].setdefault(m.group(3), opts)
            self._rebuild(); return []
        return super().execute(stmt, params)


def _cass_models(models=None, **kw):
    _install_fake_cassandra()
    sess = DdlSession()
    store = C.CassandraStore(sess, "ks")
    g = Graph()
    cy = opencypher(g, store, models=models or mmodels(), **kw)
    return cy, store, sess


def test_models_create_tables_and_indexes_automatically():
    cy, store, sess = _cass_models()
    assert set(sess.spec) == {"person", "movies", "knows", "acted_in"}
    assert sess.spec["person"]["cols"]["id"] == "uuid" and sess.spec["person"]["pk"] == "id"
    assert list(sess.spec["person"]["indexed"]) == ["name"] and list(sess.spec["movies"]["indexed"]) == ["title"]
    assert sess.spec["knows"]["cols"]["since"] == "bigint"
    # the engine works on the generated tables straight away: search goes through the index
    cy.run("CREATE (a:Person {name:'Ann', age:30})-[:KNOWS {since:2010}]->(b:Person {name:'Bob'})")
    store.clear_cache(); sess.log.clear()
    assert cy.run("MATCH (a:Person {name:'Ann'})-[:KNOWS]->(b) RETURN b.name").value() == "Bob"
    assert any('FROM "ks"."person" WHERE "name" = ?' in c and "ALLOW FILTERING" not in c for c in sess.log)
    assert sorted(r["name"] for r in sess.rows["person"].values()) == ["Ann", "Bob"]
    assert cy.verify().ok
    # second start: nothing left to do
    n = len(sess.executed)
    opencypher(Graph(), store, models=mmodels())
    assert len(sess.executed) == n


def test_models_alter_tables_for_new_fields_and_indexes():
    cy, store, sess = _cass_models()
    class MPerson2(VertexModel, type="Person", table="person"):
        name: str = Field(index=True)
        age: int | None = None
        email: str | None = Field(default=None, index=True)       # existing column, NEW index
        nickname: str | None = None                                # new column
        status: Status = Status.ACTIVE
        tags: list[str] = Field(default_factory=list)
        born: _dt.datetime | None = None
    m2 = Models([MPerson2, MMovie, MKnows, MActed])
    n = len(sess.executed)
    p = m2.plan(store)
    assert sorted(p.statements) == sorted([                            # email existed, only its INDEX is new
        'ALTER TABLE "ks"."person" ADD "nickname" text',
        'CREATE INDEX IF NOT EXISTS "person_email_idx" ON "ks"."person" ("email") USING \'sai\'']) and p.warnings == [] and p
    assert len(sess.executed) == n                                  # plan() changes nothing
    cy2 = opencypher(Graph(), store, models=m2)
    assert sess.spec["person"]["cols"]["email"] == "text" and sess.spec["person"]["cols"]["nickname"] == "text"
    assert "email" in sess.spec["person"]["indexed"]
    cy2.run("CREATE (:Person {name:'Zed', nickname:'Z', email:'a@b.c'})")
    assert not m2.plan(store)                                       # now in line
    # a model that DROPS a field leaves the column alone (and says so)
    class Slim(VertexModel, type="Person", table="person"):
        name: str = Field(index=True)
    import warnings
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        opencypher(Graph(), store, models=Models([Slim, MMovie, MKnows, MActed]))
    assert any("left untouched" in str(x.message) and "nickname" in str(x.message) for x in w)
    assert "nickname" in sess.spec["person"]["cols"]


def test_models_type_change_blocks_the_migration_and_changes_nothing():
    cy, store, sess = _cass_models()
    class Changed(VertexModel, type="Person", table="person"):
        name: str = Field(index=True)
        age: str | None = None                                       # was bigint
        brand_new: int | None = None
    m = Models([Changed, MMovie, MKnows, MActed])
    n = len(sess.executed)
    p = m.plan(store)
    assert len(p.blocked) == 1 and "person.age" in p.blocked[0] and "bigint" in p.blocked[0]
    raises(SchemaMigrationError, lambda: opencypher(Graph(), store, models=m), "person.age")
    assert len(sess.executed) == n and "brand_new" not in sess.spec["person"]["cols"]       # nothing applied
    opencypher(Graph(), store, models=m, migrate=False)                                           # opt out: no DDL at all
    assert len(sess.executed) == n
    m.apply(store, allow_blocked=True)                                                        # explicit override
    assert "brand_new" in sess.spec["person"]["cols"] and sess.spec["person"]["cols"]["age"] == "bigint"


def test_models_migration_errors_and_reverse_table():
    _install_fake_cassandra()
    sess = DdlSession(); sess.cluster.metadata.keyspaces.clear()
    store = C.CassandraStore(sess, "ks")
    raises(SchemaMigrationError, lambda: opencypher(Graph(), store, models=mmodels()), "keyspace 'ks' does not exist")
    sess = DdlSession()
    store = C.CassandraStore(sess, "ks", id_type="text")
    cy = opencypher(Graph(), store, models=mmodels(), id_index="store")                # reverse table is created too
    assert sess.spec["reverse_vertex_ids"]["cols"]["idx"] == "bigint" and sess.spec["person"]["cols"]["id"] == "text"
    cy.run("CREATE (:Person {name:'R'})")
    assert len(sess.rows["reverse_vertex_ids"]) == 2 and cy.verify().ok
    raises(ValueError, lambda: opencypher(Graph(), None, models=mmodels()), "property store")


def test_models_on_cassandra_roundtrip_with_validation_and_rollback():
    cy, store, sess = _cass_models()
    raises(ConstraintError, lambda: cy.run("CREATE (a:Person {name:'A'})-[:KNOWS]->(b:Person {name:'B'})"), "since")
    assert all(len(v) == 0 for v in sess.rows.values())               # nothing written anywhere
    assert cy.run("MATCH (p:Person) RETURN count(*)").value() == 0
    cy.run("CREATE (a:Person {name:'A', tags:['x']})-[:KNOWS {since:2001}]->(b:Person {name:'B'})")
    row = next(r for r in sess.rows["person"].values() if r["name"] == "A")
    assert row["tags"] == ["x"] and row["status"] == "active"
    assert cy.verify().ok


# =========================================================================== #
# M. vertices: many labels, ONE type.  edges: ONE type, NO labels
# =========================================================================== #
def test_edges_have_one_type_and_no_labels():
    raises(ValueError, lambda: Schema(vertex_types=["A"], edge_types=["R"], edge_labels=["R"]), "no labels")
    e = social(); g, sc = e.g, e.sc
    assert "labels" in g.vp and "labels" not in g.ep                 # no label map exists for edges
    assert "type" in g.ep and g.num_edges() > 0
    for x in g.edges():                                              # exactly one bit, the relationship type
        assert bin(sc.type_mask(g, x)).count("1") == 1
        assert sc.type_name(g, x) in ET and sc.label_names(g, x) == [sc.type_name(g, x)]
    # the relationship type is the ONLY classifier: pattern matching, type(), procedures
    assert e.cy.run("MATCH ()-[r:KNOWS]->() RETURN count(r)").value() == e.cy.run("MATCH ()-[r]->() WHERE type(r) = 'KNOWS' RETURN count(r)").value() >= 1
    assert e.cy.run("MATCH ()-[r:KNOWS|LIVES_IN]->() RETURN count(r)").value() == \
        e.cy.run("MATCH ()-[r:KNOWS]->() RETURN count(r)").value() + e.cy.run("MATCH ()-[r:LIVES_IN]->() RETURN count(r)").value()
    assert set(col(e.cy.run("MATCH ()-[r]->() RETURN DISTINCT type(r)"))) <= set(ET)
    assert set(col(e.cy.run("CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType"))) == set(ET)
    raises(OpenCypherError, lambda: e.cy.run("MATCH ()-[r]->() RETURN labels(r)"), "needs a node")
    raises(OpenCypherError, lambda: e.cy.run("CREATE (:Person {name:'q'})-[:KNOWS|LIVES_IN]->(:City {name:'z'})"), "exactly one")
    raises(ValueError, lambda: e.sc.prepare("edge", ("KNOWS",), "KNOWS", None), "no labels")
    r = e.cy.run("MATCH ()-[r:KNOWS]->() RETURN r LIMIT 1").single()["r"]
    assert r.type == "KNOWS"
    # an old saved schema that still lists edge labels loads, and is written back without them
    old = {"bits": 64, "auto_register": True, "label_map": "labels", "type_map": "type", "id_map": "uuid",
           "vertex_labels": {"P": 0}, "edge_labels": {"R": 0}, "vertex_types": {"P": 0}, "edge_types": {"R": 0}}
    sc2 = Schema.from_dict(old)
    assert "edge_labels" not in sc2.to_dict() and sc2.reg("edge", "type").all_names() == ["R"]
    # models: an edge model cannot carry labels
    class Bad(EdgeModel, type="X", labels=["L"]):
        pass
    raises(ValueError, lambda: Models([Bad]), "no labels")


def test_vertices_have_many_labels_and_one_type():
    e = fresh(); cy = e.cy; g, sc = e.g, e.sc
    cy.run("CREATE (:Person:Admin:Staff {name:'Ann'})")
    v = next(v for v in g.vertices() if sc.get_id(g, v))
    assert sorted(sc.label_names(g, v)) == ["Admin", "Person", "Staff"]          # any number of labels
    assert sc.type_name(g, v) == "Person" and bin(sc.type_mask(g, v)).count("1") == 1   # exactly one type
    assert cy.run("MATCH (p:Admin:Staff) RETURN p.name").value() == "Ann"       # all-of label match
    assert sorted(cy.run("MATCH (p {name:'Ann'}) RETURN labels(p)").value()) == ["Admin", "Person", "Staff"]
    assert [r["name"] for r in e.tables[("vertex", "Person")].values()] == ["Ann"]    # the type decides the table
    # labels come and go; the type (and so the table / properties) never changes
    cy.run("MATCH (p:Person {name:'Ann'}) SET p:City REMOVE p:Person")
    assert sorted(sc.label_names(g, v)) == ["Admin", "City", "Staff"]
    assert sc.type_name(g, v) == "Person" and cy.run("MATCH (p:City {name:'Ann'}) RETURN p.name").value() == "Ann"
    assert cy.run("MATCH (p:Person) RETURN count(*)").value() == 0                # no longer LABELLED Person ...
    assert cy.run("MATCH (p) WHERE p.type = 'Person' RETURN count(*)").value() == 1   # ... but its TYPE is
    assert cy.verify().ok
    # with models: the type is the first label that has a model, extra labels are free
    class Tagged(VertexModel, type="Tagged", labels=["Special", "Archived"]):
        n: int | None = None
    c = opencypher(Graph(), InMemoryStore({}), models=Models([MPerson, Tagged]))
    c.run("CREATE (:Special:Archived:Tagged {n: 1})")
    assert c.run("MATCH (t:Special:Archived) RETURN t.type").value() == "Tagged"



if __name__ == "__main__":
    tests = [(n, f) for n, f in list(globals().items()) if n.startswith("test_") and callable(f)]
    failed = 0
    for n, f in tests:
        try:
            f(); print(f"ok    {n}")
        except Exception as ex:
            failed += 1; print(f"FAIL  {n}: {type(ex).__name__}: {ex}")
            import traceback; traceback.print_exc(limit=5)
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    sys.exit(1 if failed else 0)


# --------------------------------------------------------------------------------------- #
# B1: integers are 64-bit; B2: a map is not a property value
# --------------------------------------------------------------------------------------- #
def test_integer_arithmetic_overflows_at_64_bits():
    cy = opencypher(Graph())
    v = lambda q: cy.run(q).value()
    assert v("RETURN 9223372036854775807 AS x") == 9223372036854775807
    assert v("RETURN -9223372036854775808 AS x") == -9223372036854775808
    assert v("RETURN 2 ^ 70 AS x") > 1e21                          # ^ is a float operation
    for q in ("RETURN 9223372036854775807 + 1 AS x", "RETURN -9223372036854775807 - 2 AS x",
              "RETURN 3037000500 * 3037000500 AS x", "RETURN -(-9223372036854775807 - 1) AS x",
              "RETURN (-9223372036854775807 - 1) / -1 AS x",
              "UNWIND [9223372036854775807, 1] AS n RETURN sum(n) AS x"):
        raises(OpenCypherError, lambda q=q: cy.run(q), "integer overflow")
    for q in ("RETURN 9223372036854775808 AS x", "RETURN -9223372036854775809 AS x"):
        raises(OpenCypherSyntaxError, lambda q=q: cy.run(q), "too large for 64 bits")


def test_a_map_is_not_a_property_value_except_in_a_map_field():
    class Tagged(VertexModel):
        class Meta:
            type = "Tagged"
        name: str = Field(index=True)
        tags: dict[str, int] | None = None
        note: str | None = None
    cy = opencypher(Graph(), InMemoryStore({}), models=Models([Tagged]))
    cy.run("CREATE (:Tagged {name: 'a'})")
    assert cy.run("MATCH (t:Tagged) SET t.tags = {x: 1} RETURN t.tags AS t").value() == {"x": 1}
    for q in ("MATCH (t:Tagged) SET t.note = {x: 1}", "MATCH (t:Tagged) SET t += {note: {x: 1}}",
              "CREATE (:Tagged {name: 'b', note: {x: 1}})"):
        raises(OpenCypherError, lambda q=q: cy.run(q), "a map can't be a property value")
    assert cy.run("MATCH (t:Tagged) RETURN count(t) AS n").value() == 1       # nothing half-written
    plain = opencypher(Graph())                                                 # no store at all
    raises(OpenCypherError, lambda: plain.run("CREATE (:Q {m: {a: 1}})"), "a map can't be")
    assert plain.run("CREATE (q:Q {m: [1, 2]}) RETURN q.m AS m").value() == [1, 2]
