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


"""Tests that need docker-cassandra skip when it isn't reachable, so a laptop without it can still
run the rest. In CI that would hide a broken setup behind a green run: REQUIRE_CASSANDRA=1 turns
the skip into a failure."""
import os

import pytest


def cassandra_missing(reason: str = "docker-cassandra is not reachable on 127.0.0.1:9042"):
    if os.environ.get("REQUIRE_CASSANDRA") == "1":
        pytest.fail(f"{reason} (and REQUIRE_CASSANDRA=1)", pytrace=False)
    pytest.skip(reason)
