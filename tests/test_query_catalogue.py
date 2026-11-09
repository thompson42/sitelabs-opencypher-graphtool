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

"""docs/query-catalogue.md: every query parses, the console's sections hold only reads (the
console refuses writes) and the writes section only writes, every parameter block is JSON, and
the language has the one name the projects that copy it require. Running the queries needs
sitelabs-graph-database's data model and sample graphs, so that is checked there."""
import json
import re
from pathlib import Path

import pytest

from opencypher_graphtool import OpenCypher
from opencypher_graphtool.parser import parse

DOC = Path(__file__).resolve().parent.parent / "docs" / "query-catalogue.md"
TEXT = DOC.read_text()
READS, WRITES = TEXT.split("\n## Writes\n")
BLOCK = re.compile(r"```(\w+)\n(.*?)```", re.S)


def queries(text: str) -> list[str]:
    return [body.strip() for kind, body in BLOCK.findall(text) if kind == "opencypher"]


def test_the_catalogue_has_queries_and_only_known_blocks():
    assert len(queries(READS)) > 100 and len(queries(WRITES)) >= 10
    assert {kind for kind, _ in BLOCK.findall(TEXT)} <= {"opencypher", "json", "bash"}


@pytest.mark.parametrize("query", queries(READS))
def test_a_console_query_parses_and_reads(query):
    q = parse(query)
    assert q.explain or not OpenCypher.is_write(q), "the console refuses writes: move it to ## Writes"


@pytest.mark.parametrize("query", queries(WRITES))
def test_a_write_parses_and_writes(query):
    assert OpenCypher.is_write(parse(query))


def test_every_parameter_block_is_json_after_a_query():
    blocks = BLOCK.findall(TEXT)
    for i, (kind, body) in enumerate(blocks):
        if kind == "json":
            assert blocks[i - 1][0] == "opencypher", body
            assert isinstance(json.loads(body), dict)


def test_it_names_no_project_that_copies_it():
    """Each project keeps a word-for-word copy, so the catalogue mustn't name any of them."""
    assert not re.search(r"(?i)sitelabs-(graph-database|partitioned-graph)", TEXT)


def test_the_language_has_one_name():
    """The copies in sitelabs-partitioned-graph and sitelabs-graph-database-conda are checked for it."""
    word = "cy" + "pher"
    assert not re.search(rf"(?i)(?<!open){word}", TEXT)
