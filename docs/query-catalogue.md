# openCypher query catalogue

> **For:** anyone trying the engine's openCypher in the UI query console (sitelabs-graph-ui). **You'll
> find:** one or more queries for each part of the language the engine supports, ready to copy into
> the console's query box, and what each one shows.

Every query below runs as written against two sample graphs, a karate club and a citation graph (two
return no rows on purpose, and say so). Where the project serving the console ships them, load them
once, from its directory:

```bash
python -m example_solutions.example_graphs.karate_club   # 34 :Member nodes, 78 :FRIENDS_WITH relationships
python -m example_solutions.example_graphs.citations     # 48 :Document nodes (with embeddings), 77 :CITES relationships
```

* **Karate club:** `(:Member {member_no, name, role, faction})-[:FRIENDS_WITH]->(:Member)`. Member 1
  is "Mr. Hi", member 34 is "John A.". Each friendship is stored once, lower `member_no` to higher, so
  most queries match it in either direction: `-[:FRIENDS_WITH]-`.
* **Citations:** `(:Document {title, topic, year, embedding})-[:CITES]->(:Document)`. Four topics, 12
  papers each, from 2015 to 2026. `embedding` is a 16-number vector.

**Parameters.** A query that uses `$name` has its parameters in a JSON block under it: paste that into
the console's parameters box.

**Writes.** The console is read-only and refuses any query that would change the graph. The queries in
[Writes](#writes) are for `POST /opencypher`; in the console, press **Explain** on them to see their
plan without running them.

What the engine does *not* support is listed in the documentation of the project serving the
console.

* [Catalogue procedures](#catalogue-procedures)
* [Literals](#literals)
* [Operators](#operators)
* [Null and three-valued logic](#null-and-three-valued-logic)
* [Lists](#lists)
* [Maps](#maps)
* [CASE](#case)
* [Functions](#functions)
* [MATCH and patterns](#match-and-patterns)
* [Variable-length patterns and paths](#variable-length-patterns-and-paths)
* [Shortest paths](#shortest-paths)
* [OPTIONAL MATCH](#optional-match)
* [Pattern predicates, EXISTS and pattern comprehensions](#pattern-predicates-exists-and-pattern-comprehensions)
* [WITH](#with)
* [RETURN, ORDER BY, SKIP and LIMIT](#return-order-by-skip-and-limit)
* [Aggregation](#aggregation)
* [UNWIND](#unwind)
* [UNION](#union)
* [Parameters](#parameters)
* [Graph functions](#graph-functions)
* [Graph algorithm procedures](#graph-algorithm-procedures)
* [Vector search](#vector-search)
* [EXPLAIN](#explain)
* [Writes](#writes)

## Catalogue procedures

The labels, relationship types and property keys in the graph:

```opencypher
CALL db.labels() YIELD label RETURN label
```

```opencypher
CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType
```

```opencypher
CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey ORDER BY propertyKey
```

## Literals

Numbers: integers, hexadecimal, floats, exponents and negatives:

```opencypher
RETURN 42 AS int, 0x1F AS hex, 3.14 AS float, 6.02e23 AS exponent, 1e-3 AS small, -7 AS negative
```

Strings in single or double quotes, with escapes (`\'`, `\"`, `\n`, `\t`, `\\`, `\uXXXX`):

```opencypher
RETURN 'single' AS a, "double" AS b, 'it\'s' AS c, "say \"hi\"" AS d, 'tab:\there' AS e, 'café' AS f
```

Booleans, null, lists and maps, nested:

```opencypher
RETURN true AS t, false AS f, null AS n, [1, 'two', 3.0, [4]] AS list, {name: 'Ann', tags: ['a', 'b'], address: {city: 'Sydney'}} AS map
```

Comments, and backtick-quoted names (with spaces, or that would be keywords):

```opencypher
// a line comment
MATCH (`the member`:Member) /* a block comment */
WHERE `the member`.member_no = 1
RETURN `the member`.name AS `Member name`, 1 AS `match`
```

## Operators

Arithmetic: `+ - * / % ^` and unary minus. Integer division truncates; `^` always gives a float:

```opencypher
RETURN 7 + 2 AS add, 7 - 2 AS sub, 7 * 2 AS mul, 7 / 2 AS int_div, 7.0 / 2 AS float_div, 7 % 2 AS mod, 2 ^ 10 AS pow, -(3 + 4) AS neg
```

Precedence and parentheses:

```opencypher
RETURN 2 + 3 * 4 AS a, (2 + 3) * 4 AS b, -2 ^ 2 AS c, 2 ^ 3 ^ 2 AS right_assoc
```

`+` joins strings and lists too:

```opencypher
RETURN 'graph' + '-' + 'tool' AS s, [1, 2] + [3] AS l, [1, 2] + 3 AS l2
```

Comparisons, including chained ones (`a < b <= c` means `a < b AND b <= c`):

```opencypher
RETURN 1 = 1 AS eq, 1 <> 2 AS ne, 1 != 2 AS ne2, 1 < 2 AS lt, 2 <= 2 AS le, 3 > 2 AS gt, 3 >= 4 AS ge, 1 < 2 <= 2 < 3 AS chained, 'a' < 'b' AS strings
```

Boolean operators:

```opencypher
RETURN true AND false AS and_, true OR false AS or_, true XOR true AS xor_, NOT false AS not_
```

String operators: `STARTS WITH`, `ENDS WITH`, `CONTAINS` and regular expressions (`=~`, the whole
string must match):

```opencypher
MATCH (m:Member)
WHERE m.name STARTS WITH 'Member 2' OR m.name ENDS WITH '.' OR m.name CONTAINS 'Hi'
RETURN m.member_no, m.name ORDER BY m.member_no
```

```opencypher
MATCH (d:Document) WHERE d.title =~ '(?i)quorum.*' RETURN d.title, d.year
```

`IN` a list:

```opencypher
MATCH (m:Member) WHERE m.member_no IN [1, 2, 33, 34] RETURN m.member_no, m.name, m.role
```

## Null and three-valued logic

`null` propagates through comparisons and arithmetic, and `AND` / `OR` follow three-valued logic:

```opencypher
RETURN null = null AS eq, null <> 1 AS ne, null + 1 AS plus, null OR true AS or_true, null OR false AS or_false, null AND false AS and_false, NOT null AS not_null
```

`IN` with a null in the list, and `IS NULL` / `IS NOT NULL`:

```opencypher
RETURN 2 IN [1, 2, null] AS found, 3 IN [1, 2, null] AS unknown, null IS NULL AS a, 1 IS NOT NULL AS b
```

A missing property is null:

```opencypher
MATCH (m:Member {member_no: 1}) RETURN m.nickname AS missing, m.nickname IS NULL AS is_null, coalesce(m.nickname, m.name) AS shown
```

## Lists

Indexing (negative counts from the end) and slicing (`[from..to]`, `to` exclusive, either may be left
out):

```opencypher
WITH [10, 20, 30, 40, 50] AS l
RETURN l[0] AS first, l[-1] AS last, l[1..3] AS middle, l[..2] AS head2, l[-2..] AS tail2, l[10] AS out_of_range
```

List comprehensions: filter, map, or both:

```opencypher
WITH range(1, 10) AS l
RETURN [x IN l WHERE x % 2 = 0] AS evens, [x IN l | x * x] AS squares, [x IN l WHERE x > 7 | x * 10] AS both
```

Quantifiers `all`, `any`, `none`, `single`:

```opencypher
WITH [2, 4, 6, 7] AS l
RETURN all(x IN l WHERE x > 0) AS all_pos, any(x IN l WHERE x % 2 = 1) AS any_odd, none(x IN l WHERE x > 100) AS none_big, single(x IN l WHERE x = 7) AS one_seven
```

`reduce`:

```opencypher
RETURN reduce(total = 0, x IN range(1, 100) | total + x) AS sum_1_to_100, reduce(s = '', w IN ['a', 'b', 'c'] | s + w) AS joined
```

List functions:

```opencypher
RETURN range(0, 10, 3) AS stepped, range(5, 1, -2) AS down, size([1, 2, 3]) AS size, head([1, 2, 3]) AS head, last([1, 2, 3]) AS last, tail([1, 2, 3]) AS tail, reverse([1, 2, 3]) AS reversed, isEmpty([]) AS empty
```

Lists from the graph, compared with comprehensions over them:

```opencypher
MATCH (d:Document {topic: 'query languages'})
WITH collect(d.year) AS years
RETURN years, [y IN years WHERE y >= 2022] AS recent, size(years) AS n
```

## Maps

Keys by `.name` or `['name']`, nested access, and `keys()`:

```opencypher
WITH {name: 'Ann', tags: ['a', 'b'], address: {city: 'Sydney'}} AS m
RETURN m.name AS name, m['tags'][1] AS second_tag, m.address.city AS city, keys(m) AS keys
```

A property chosen at run time, with `n[expression]`:

```opencypher
MATCH (m:Member {member_no: 1}) RETURN m[$key] AS value, m['role'] AS role
```

```json
{"key": "faction"}
```

Building a map from a node (the engine has no map projection `n{.a, .b}`, so write the map out):

```opencypher
MATCH (m:Member) WHERE m.member_no <= 3
RETURN {no: m.member_no, name: m.name, faction: m.faction} AS member, properties(m) AS all_properties
```

## CASE

The generic form (conditions):

```opencypher
MATCH (d:Document)
RETURN d.title,
       CASE WHEN d.year < 2018 THEN 'early' WHEN d.year < 2023 THEN 'middle' ELSE 'recent' END AS era
ORDER BY d.year LIMIT 10
```

The simple form (one value against several):

```opencypher
MATCH (m:Member)
RETURN m.member_no, CASE m.role WHEN 'instructor' THEN 'teaches' WHEN 'administrator' THEN 'runs the club' ELSE 'trains' END AS does
ORDER BY m.member_no LIMIT 5
```

## Functions

Strings:

```opencypher
RETURN toUpper('graph') AS up, toLower('GRAPH') AS low, trim('  x  ') AS trim, ltrim('  x') AS ltrim, rtrim('x  ') AS rtrim,
       replace('a-b-c', '-', '+') AS replace, substring('openCypher', 4) AS sub, substring('openCypher', 0, 4) AS sub2,
       left('openCypher', 4) AS left, right('openCypher', 6) AS right, split('a,b,c', ',') AS split, reverse('abc') AS rev, size('hello') AS size
```

Numbers:

```opencypher
RETURN abs(-3) AS abs, ceil(1.2) AS ceil, floor(1.8) AS floor, round(2.5) AS round, round(3.14159, 2) AS round2,
       round(2.5, 0, 'HALF_EVEN') AS banker, sign(-4) AS sign, sqrt(16) AS sqrt, exp(1) AS exp, log(e()) AS log, log10(1000) AS log10
```

Trigonometry and constants:

```opencypher
RETURN pi() AS pi, e() AS e, sin(pi() / 2) AS sin, cos(0) AS cos, tan(0) AS tan, asin(1) AS asin, acos(1) AS acos,
       atan(1) AS atan, atan2(1, 1) AS atan2, degrees(pi()) AS degrees, radians(180) AS radians
```

Conversions:

```opencypher
RETURN toInteger('42') AS i, toInteger(3.9) AS trunc, toFloat('2.5') AS f, toBoolean('true') AS b, toString(12) AS s, toString(true) AS s2, toInteger('nope') AS bad
```

Others: `coalesce`, `timestamp` (milliseconds since 1970), `randomUUID` and `rand` (0 to 1):

```opencypher
RETURN coalesce(null, null, 'third') AS first_non_null, timestamp() AS now_ms, randomUUID() AS uuid, rand() AS random
```

## MATCH and patterns

All nodes with a label, and inline property maps:

```opencypher
MATCH (m:Member) RETURN m ORDER BY m.member_no LIMIT 5
```

```opencypher
MATCH (m:Member {faction: 'Officer', role: 'member'}) RETURN m.member_no, m.name ORDER BY m.member_no
```

Any node, with a label test in `WHERE`:

```opencypher
MATCH (n) WHERE n:Document AND n.year = 2015 RETURN labels(n) AS labels, n.title
```

Several labels at once (`:A:B`): no sample node has two, so this returns nothing, but it shows the
syntax in both places:

```opencypher
MATCH (n:Member:Person) RETURN n UNION MATCH (n) WHERE n:Member:Document RETURN n
```

Relationship directions: outgoing, incoming, either way, and anonymous relationships:

```opencypher
MATCH (d:Document {title: 'Pattern matching, part 2'})-[:CITES]->(cited) RETURN 'cites' AS dir, cited.title AS title
UNION ALL
MATCH (d:Document {title: 'Pattern matching, part 2'})<-[:CITES]-(citing) RETURN 'cited by' AS dir, citing.title AS title
```

```opencypher
MATCH (:Member {member_no: 1})-[:FRIENDS_WITH]-(friend) RETURN friend.member_no ORDER BY friend.member_no
```

```opencypher
MATCH (a:Member {member_no: 34})--(b) RETURN count(b) AS either, size([(a)-->(x) | x]) AS out, size([(a)<--(x) | x]) AS in
```

A relationship variable, its type and properties, and type alternatives `[:A|B]`:

```opencypher
MATCH (a)-[r:FRIENDS_WITH|CITES]->(b) RETURN type(r) AS type, count(*) AS n
```

```opencypher
MATCH (a:Member {member_no: 1})-[r:FRIENDS_WITH]->(b) RETURN r, startNode(r).name AS from, endNode(r).name AS to LIMIT 3
```

Longer chains: friends of friends that are not direct friends:

```opencypher
MATCH (me:Member {member_no: 17})-[:FRIENDS_WITH]-(f)-[:FRIENDS_WITH]-(fof)
WHERE fof <> me AND NOT (me)-[:FRIENDS_WITH]-(fof)
RETURN DISTINCT fof.member_no AS suggestion ORDER BY suggestion
```

Cycles: every friendship triangle, each once:

```opencypher
MATCH (a:Member)-[:FRIENDS_WITH]-(b:Member)-[:FRIENDS_WITH]-(c:Member)-[:FRIENDS_WITH]-(a)
WHERE a.member_no < b.member_no < c.member_no
RETURN a.member_no, b.member_no, c.member_no ORDER BY a.member_no, b.member_no, c.member_no
```

Several pattern parts separated by commas, sharing a node:

```opencypher
MATCH (hi:Member {member_no: 1})-[:FRIENDS_WITH]-(x), (x)-[:FRIENDS_WITH]-(john:Member {member_no: 34})
RETURN x.member_no AS friend_of_both, x.faction
```

Several `MATCH` clauses in a row:

```opencypher
MATCH (a:Member {member_no: 1})
MATCH (b:Member {member_no: 34})
RETURN a.name, b.name, (a)-[:FRIENDS_WITH]-(b) AS are_friends
```

## Variable-length patterns and paths

Exactly 2 hops (`*2`), a range (`*1..2`), at most (`*..2`), and at least (`*2..`, capped by the server):

```opencypher
MATCH (:Member {member_no: 17})-[:FRIENDS_WITH*2]-(m) RETURN DISTINCT m.member_no ORDER BY m.member_no
```

```opencypher
MATCH (:Member {member_no: 17})-[:FRIENDS_WITH*1..2]-(m) RETURN count(DISTINCT m) AS within_two_hops
```

```opencypher
MATCH (:Member {member_no: 17})-[:FRIENDS_WITH*..2]-(m) RETURN count(DISTINCT m) AS within_two_hops
```

```opencypher
MATCH (d:Document {title: 'Variable-length paths, part 2'})-[:CITES*3..]->(old) RETURN DISTINCT old.title, old.year ORDER BY old.year LIMIT 5
```

Any number of hops (`*`) along the citations, with a path variable, and the path functions:

```opencypher
MATCH p = (d:Document {title: 'Variable-length paths, part 2'})-[:CITES*]->(old:Document {year: 2015})
RETURN p, length(p) AS hops, [n IN nodes(p) | n.year] AS years, [r IN relationships(p) | type(r)] AS types
ORDER BY hops LIMIT 3
```

A path variable on a fixed-length pattern, and a relationship-list variable on a variable-length one:

```opencypher
MATCH p = (a:Member {member_no: 1})-[:FRIENDS_WITH]->(b)-[:FRIENDS_WITH]->(c)
RETURN p, length(p) AS len LIMIT 5
```

```opencypher
MATCH (a:Member {member_no: 1})-[rs:FRIENDS_WITH*2]-(c:Member {member_no: 34})
RETURN [r IN rs | [startNode(r).member_no, endNode(r).member_no]] AS steps
```

## Shortest paths

`shortestPath()` (one) and `allShortestPaths()` (every one of the shortest length):

```opencypher
MATCH p = shortestPath((a:Member {member_no: 17})-[:FRIENDS_WITH*]-(b:Member {member_no: 26}))
RETURN p, length(p) AS hops, [n IN nodes(p) | n.member_no] AS via
```

```opencypher
MATCH p = allShortestPaths((a:Member {member_no: 17})-[:FRIENDS_WITH*]-(b:Member {member_no: 26}))
RETURN [n IN nodes(p) | n.member_no] AS via ORDER BY via
```

With a hop limit, and following direction:

```opencypher
MATCH p = shortestPath((a:Document {title: 'Variable-length paths, part 2'})-[:CITES*..10]->(b:Document {year: 2015}))
RETURN b.title, length(p) AS hops ORDER BY hops, b.title
```

## OPTIONAL MATCH

Keeps the row, with nulls, when the pattern isn't found: the first paper cites nothing:

```opencypher
MATCH (d:Document) WHERE d.year <= 2016
OPTIONAL MATCH (d)-[:CITES]->(c:Document)
RETURN d.title, d.year, count(c) AS cites, collect(c.title) AS titles ORDER BY d.year, d.title
```

```opencypher
MATCH (m:Member {member_no: 12})
OPTIONAL MATCH (m)-[:FRIENDS_WITH]-(f:Member) WHERE f.member_no > 1
RETURN m.member_no, f.member_no AS other_friend
```

## Pattern predicates, EXISTS and pattern comprehensions

A pattern as a condition, and its negation:

```opencypher
MATCH (m:Member) WHERE (m)-[:FRIENDS_WITH]-(:Member {member_no: 1}) AND (m)-[:FRIENDS_WITH]-(:Member {member_no: 34})
RETURN m.member_no AS friend_of_both_leaders
```

```opencypher
MATCH (d:Document) WHERE NOT (d)<-[:CITES]-() RETURN d.title AS never_cited, d.year ORDER BY d.year DESC LIMIT 10
```

`EXISTS { pattern }`, with or without a `WHERE`:

```opencypher
MATCH (d:Document)
WHERE EXISTS { (d)-[:CITES]->(other:Document) WHERE other.topic <> d.topic }
RETURN d.title AS cites_another_topic, d.topic ORDER BY d.title
```

Pattern comprehensions: a list built from a pattern, per row:

```opencypher
MATCH (m:Member) WHERE m.member_no IN [1, 34]
RETURN m.name, [(m)-[:FRIENDS_WITH]-(f) WHERE f.faction <> m.faction | f.member_no] AS friends_across_the_split
```

Counting with one (the engine has no `COUNT { }` subquery):

```opencypher
MATCH (m:Member)
RETURN m.member_no, size([(m)-[:FRIENDS_WITH]-(f) | f]) AS friends ORDER BY friends DESC LIMIT 5
```

## WITH

Passing values on, filtering after an aggregate (like SQL's `HAVING`):

```opencypher
MATCH (m:Member)-[:FRIENDS_WITH]-(f)
WITH m, count(f) AS friends
WHERE friends >= 10
RETURN m.member_no, m.name, friends ORDER BY friends DESC
```

`ORDER BY` and `LIMIT` inside `WITH`, before matching on:

```opencypher
MATCH (d:Document) WITH d ORDER BY d.year DESC, d.title LIMIT 1
MATCH (d)-[:CITES]->(c)
RETURN d.title AS newest, collect(c.title) AS it_cites
```

`WITH DISTINCT`, and `WITH *` (keep every variable, add more):

```opencypher
MATCH (d:Document) WITH DISTINCT d.topic AS topic RETURN topic ORDER BY topic
```

```opencypher
MATCH (a:Member {member_no: 1})-[:FRIENDS_WITH]-(b)
WITH *, b.member_no * 10 AS score
RETURN a.member_no, b.member_no, score ORDER BY score LIMIT 3
```

Chaining several `WITH` stages:

```opencypher
MATCH (d:Document)
WITH d.topic AS topic, min(d.year) AS first, max(d.year) AS last
WITH topic, last - first AS span
WHERE span > 5
RETURN topic, span ORDER BY topic
```

## RETURN, ORDER BY, SKIP and LIMIT

`RETURN *`, `RETURN DISTINCT`, aliases, expressions:

```opencypher
MATCH (m:Member {member_no: 34}) RETURN *
```

```opencypher
MATCH (m:Member) RETURN DISTINCT m.faction AS faction, m.role AS role ORDER BY faction, role
```

Several sort keys, ascending and descending (`ASC`, `ASCENDING`, `DESC`, `DESCENDING`):

```opencypher
MATCH (d:Document) RETURN d.topic, d.year, d.title ORDER BY d.topic ASCENDING, d.year DESC LIMIT 8
```

Paging with `SKIP` and `LIMIT`, including expressions:

```opencypher
MATCH (m:Member) RETURN m.member_no ORDER BY m.member_no SKIP 10 LIMIT 5
```

```opencypher
MATCH (m:Member) RETURN m.member_no ORDER BY m.member_no SKIP $page * $size LIMIT $size
```

```json
{"page": 2, "size": 5}
```

Nulls sort last when ascending (first when descending):

```opencypher
UNWIND [3, null, 1, 2] AS x RETURN x ORDER BY x
```

## Aggregation

`count(*)`, `count(expr)` (skips nulls), `count(DISTINCT ...)`; the other columns are the grouping keys:

```opencypher
MATCH (m:Member) RETURN m.faction AS faction, count(*) AS members, count(DISTINCT m.role) AS roles ORDER BY faction
```

```opencypher
UNWIND [1, null, 2, 2] AS x RETURN count(*) AS rows, count(x) AS values, count(DISTINCT x) AS distinct_values
```

`sum`, `avg`, `min`, `max`:

```opencypher
MATCH (d:Document) RETURN d.topic, sum(d.year) AS sum, avg(d.year) AS avg, min(d.year) AS min, max(d.title) AS last_title ORDER BY d.topic
```

`collect` and `collect(DISTINCT ...)`:

```opencypher
MATCH (m:Member)-[:FRIENDS_WITH]-(f) WHERE m.member_no IN [1, 34]
RETURN m.name, collect(f.member_no) AS friends, collect(DISTINCT f.faction) AS factions
```

Statistics: `stDev` (sample), `stDevP` (population), `percentileCont` (interpolated), `percentileDisc`
(an actual value):

```opencypher
MATCH (m:Member)-[:FRIENDS_WITH]-(f)
WITH m, count(f) AS degree
RETURN stDev(degree) AS stdev, stDevP(degree) AS stdevp, percentileCont(degree, 0.5) AS median, percentileDisc(degree, 0.9) AS p90, avg(degree) AS mean
```

Aggregates inside expressions, and aggregating a whole graph to one row:

```opencypher
MATCH (d:Document) RETURN count(*) AS papers, max(d.year) - min(d.year) + 1 AS years, round(toFloat(count(*)) / 4, 1) AS per_topic
```

## UNWIND

A list into rows, nested `UNWIND`s, and `range()`:

```opencypher
UNWIND ['a', 'b'] AS letter UNWIND range(1, 3) AS n RETURN letter + toString(n) AS cell
```

From a parameter, then matching on each value:

```opencypher
UNWIND $members AS no
MATCH (m:Member {member_no: no})
RETURN no, m.name, m.faction
```

```json
{"members": [1, 9, 34]}
```

A collected list back into rows:

```opencypher
MATCH (d:Document {topic: 'vector retrieval'})
WITH collect(d) AS docs
UNWIND docs[0..3] AS d
RETURN d.title
```

An empty list gives no rows:

```opencypher
UNWIND [] AS x RETURN x
```

## UNION

`UNION` removes duplicate rows, `UNION ALL` keeps them; the column names must match:

```opencypher
MATCH (m:Member {role: 'instructor'}) RETURN m.name AS name
UNION
MATCH (m:Member {role: 'administrator'}) RETURN m.name AS name
UNION
MATCH (m:Member {role: 'instructor'}) RETURN m.name AS name
```

```opencypher
RETURN 1 AS x UNION ALL RETURN 1 AS x UNION ALL RETURN 2 AS x
```

## Parameters

Values of every JSON type, used in a pattern and in expressions:

```opencypher
MATCH (m:Member {faction: $faction})
WHERE m.member_no IN $numbers AND m.name STARTS WITH $prefix
RETURN m.member_no, m.name, $flag AS flag, $settings.colour AS colour
```

```json
{"faction": "Mr. Hi", "numbers": [1, 2, 3, 4, 5, 20], "prefix": "Member", "flag": true, "settings": {"colour": "blue"}}
```

Finding a node by its id, as the console's **Expand neighbours** does (take an id from any answer):

```opencypher
MATCH (a)-[r]-(b) WHERE id(a) = $id RETURN a, r, b
```

```json
{"id": "paste an id here"}
```

## Graph functions

A node's id, labels, keys and properties, and a relationship's type and ends:

```opencypher
MATCH (a:Member {member_no: 1})-[r:FRIENDS_WITH]->(b:Member {member_no: 2})
RETURN id(a) AS id, elementId(a) AS element_id, labels(a) AS labels, keys(a) AS keys, properties(a) AS props,
       type(r) AS type, id(startNode(r)) = id(a) AS starts_at_a, endNode(r).name AS ends_at
```

`degree()`: all relationships, or one direction only:

```opencypher
MATCH (m:Member) RETURN m.member_no, degree(m) AS all, degree(m, 'out') AS out, degree(m, 'in') AS in ORDER BY all DESC LIMIT 5
```

`exists(property)` (from older openCypher; `IS NOT NULL` is the modern form):

```opencypher
MATCH (m:Member {member_no: 1}) RETURN exists(m.name) AS has_name, exists(m.nickname) AS has_nickname
```

`length()` of a path and `size()` of a list or string:

```opencypher
MATCH p = (:Member {member_no: 1})-[:FRIENDS_WITH]->()-[:FRIENDS_WITH]->() RETURN length(p) AS path, size(nodes(p)) AS nodes, size('abc') AS string LIMIT 1
```

## Graph algorithm procedures

These run in graph-tool over the whole graph. `YIELD` names the columns, `AS` renames them, and a
`WHERE` after `YIELD` filters them.

PageRank, on the friendships only, both directions:

```opencypher
CALL gt.pageRank({direction: 'BOTH', relationshipTypes: ['FRIENDS_WITH'], dampingFactor: 0.85})
YIELD node, score AS rank
WHERE node:Member
RETURN node.member_no, node.name, round(rank, 4) AS rank ORDER BY rank DESC LIMIT 5
```

The most cited papers by PageRank (direction follows the citations):

```opencypher
CALL gt.pageRank({relationshipTypes: ['CITES'], maxIterations: 100, tolerance: 0.000001})
YIELD node, score WHERE node:Document
RETURN node.title, node.year, round(score, 4) AS score ORDER BY score DESC LIMIT 5
```

Connected components:

```opencypher
CALL gt.connectedComponents() YIELD node, componentId
RETURN componentId, labels(node)[0] AS label, count(*) AS size ORDER BY size DESC
```

Label propagation (`gt.peerPressure`), its clusters set against the club's two factions:

```opencypher
CALL gt.peerPressure({relationshipTypes: ['FRIENDS_WITH'], maxIterations: 30}) YIELD node, cluster
WITH node, cluster WHERE node:Member
RETURN cluster, node.faction AS faction, count(*) AS members ORDER BY cluster, faction
```

A shortest path from a procedure, with matched nodes as its arguments:

```opencypher
MATCH (a:Member {member_no: 17}), (b:Member {member_no: 26})
CALL gt.shortestPath(a, b, {direction: 'BOTH', relationshipTypes: ['FRIENDS_WITH']}) YIELD path, totalCost
RETURN path, totalCost, [n IN nodes(path) | n.member_no] AS via
```

## Vector search

The parameter `q` is a 16-number stand-in for the embedding of a question about vector retrieval
(the centre of the sample's "vector retrieval" topic); it is the same under each query here.

The k most similar papers (a nearest-neighbour search in Cassandra):

```opencypher
CALL vector.search('Document', 'embedding', $q, 5) YIELD node, score
RETURN node.title AS title, node.topic AS topic, round(score, 4) AS score
```

```json
{"q": [-0.097498, -0.030158, 0.179123, 0.070401, -0.126774, -0.271176, -0.147528, 0.345993, -0.228961, 0.069361, 0.12087, -0.422174, 0.013737, 0.370172, -0.570844, -0.091135]}
```

With a filter on an indexed field, applied inside Cassandra:

```opencypher
CALL vector.search('Document', 'embedding', $q, 3, {topic: 'query languages'}) YIELD node, score
RETURN node.title AS title, round(score, 4) AS score
```

```json
{"q": [-0.097498, -0.030158, 0.179123, 0.070401, -0.126774, -0.271176, -0.147528, 0.345993, -0.228961, 0.069361, 0.12087, -0.422174, 0.013737, 0.370172, -0.570844, -0.091135]}
```

Vector search, then the graph: what the best matches cite:

```opencypher
CALL vector.search('Document', 'embedding', $q, 3) YIELD node, score
MATCH (node)-[:CITES*1..2]->(src:Document)
RETURN src.title AS cited, max(round(score, 4)) AS relevance ORDER BY relevance DESC, cited LIMIT 5
```

```json
{"q": [-0.097498, -0.030158, 0.179123, 0.070401, -0.126774, -0.271176, -0.147528, 0.345993, -0.228961, 0.069361, 0.12087, -0.422174, 0.013737, 0.370172, -0.570844, -0.091135]}
```

The graph, then similarity: the similarity functions score vectors already in hand (`vector.cosine`,
`vector.dot_product`, `vector.euclidean`, each from 0 to 1):

```opencypher
MATCH (d:Document) WITH d ORDER BY d.year DESC LIMIT 1
MATCH (d)-[:CITES*1..3]->(c:Document)
RETURN DISTINCT c.title AS title, round(vector.cosine(c.embedding, $q), 4) AS cosine,
       round(vector.dot_product(c.embedding, $q), 4) AS dot, round(vector.euclidean(c.embedding, $q), 4) AS euclidean
ORDER BY cosine DESC LIMIT 5
```

```json
{"q": [-0.097498, -0.030158, 0.179123, 0.070401, -0.126774, -0.271176, -0.147528, 0.345993, -0.228961, 0.069361, 0.12087, -0.422174, 0.013737, 0.370172, -0.570844, -0.091135]}
```

Two papers compared with each other:

```opencypher
MATCH (a:Document {title: 'Leader leases, part 1'}), (b:Document)
WHERE b <> a
RETURN b.title, b.topic, round(vector.cosine(a.embedding, b.embedding), 4) AS similarity ORDER BY similarity DESC LIMIT 5
```

## EXPLAIN

The plan, not the answer: where each pattern starts and how its start nodes are found. The console's
**Explain** button adds the `EXPLAIN` for you.

```opencypher
EXPLAIN MATCH (m:Member {member_no: 1})-[:FRIENDS_WITH]-(f) RETURN f.name
```

`EXPLAIN` of a write is a read, so the console accepts it:

```opencypher
EXPLAIN MATCH (m:Member {member_no: 1}) SET m.role = 'instructor' RETURN m
```

## Writes

**Not in the console**, which refuses writes: send these to `POST /opencypher` with a key that may
write, for example

```bash
curl -s -X POST 127.0.0.1:5050/opencypher -H 'Content-Type: application/json' \
     -d '{"query": "CREATE (p:Person {name: $name}) RETURN p", "params": {"name": "Catalogue Ann"}}'
```

They use the data model's `Person` / `KNOWS` types, with names starting `Catalogue`, and run in order;
the last one removes everything they made. In the console, **Explain** shows each one's plan.

`CREATE`: a node, a path, and a relationship between nodes already there:

```opencypher
CREATE (p:Person {name: 'Catalogue Ann', address: '1 First St'}) RETURN p
```

```opencypher
CREATE (b:Person {name: 'Catalogue Bob'})-[k:KNOWS {since: 2020}]->(c:Person {name: 'Catalogue Cat'}) RETURN b, k, c
```

```opencypher
MATCH (a:Person {name: 'Catalogue Ann'}), (b:Person {name: 'Catalogue Bob'})
CREATE (a)-[k:KNOWS {since: 2015}]->(b) RETURN a, k, b
```

`MERGE`: find or create, with `ON CREATE SET` and `ON MATCH SET` (run it twice to see both):

```opencypher
MERGE (p:Person {name: 'Catalogue Dan'})
ON CREATE SET p.address = 'created'
ON MATCH SET p.address = 'matched'
RETURN p.name, p.address
```

```opencypher
MATCH (a:Person {name: 'Catalogue Dan'}), (b:Person {name: 'Catalogue Ann'})
MERGE (a)-[k:KNOWS]->(b)
ON CREATE SET k.since = 2024
RETURN k.since
```

Many at once, from a parameter:

```opencypher
UNWIND $people AS person
MERGE (p:Person {name: person.name})
SET p.address = person.address
RETURN count(p) AS merged
```

```json
{"people": [{"name": "Catalogue Eve", "address": "5 Fifth St"}, {"name": "Catalogue Fay", "address": "6 Sixth St"}]}
```

`SET`: one property, several properties from a map (`+=` adds, `=` replaces them all), and a label:

```opencypher
MATCH (p:Person {name: 'Catalogue Ann'}) SET p.address = '2 Second St' RETURN p
```

```opencypher
MATCH (p:Person {name: 'Catalogue Bob'}) SET p += {address: '3 Third St'} RETURN properties(p)
```

```opencypher
MATCH (p:Person {name: 'Catalogue Cat'}) SET p = {name: 'Catalogue Cat', address: '4 Fourth St'} RETURN properties(p)
```

```opencypher
MATCH (p:Person {name: 'Catalogue Ann'}) SET p:Member RETURN labels(p)
```

`REMOVE`: a property and a label:

```opencypher
MATCH (p:Person {name: 'Catalogue Ann'}) REMOVE p.address, p:Member RETURN p.address, labels(p)
```

`DELETE` a relationship, then `DETACH DELETE` the nodes with their relationships:

```opencypher
MATCH (:Person {name: 'Catalogue Ann'})-[k:KNOWS]->(:Person {name: 'Catalogue Bob'}) DELETE k
```

```opencypher
MATCH (p:Person) WHERE p.name STARTS WITH 'Catalogue' DETACH DELETE p
```
