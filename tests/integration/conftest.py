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

"""Fixtures for the tests against a real Cassandra (127.0.0.1:9042, e.g. a docker-cassandra
container): a session, and a throwaway keyspace per test."""
import uuid

import pytest

from tests.cassandra_required import cassandra_missing

cassandra = pytest.importorskip("cassandra")
from cassandra.cluster import Cluster, NoHostAvailable  # noqa: E402


@pytest.fixture(scope="module")
def session():
    try:
        cluster = Cluster(["127.0.0.1"], port=9042, connect_timeout=3)
        sess = cluster.connect()
    except NoHostAvailable:
        cassandra_missing()
    yield sess
    cluster.shutdown()


@pytest.fixture
def keyspace(session):
    ks = f"it_{uuid.uuid4().hex[:10]}"
    yield ks
    session.execute(f'DROP KEYSPACE IF EXISTS "{ks}"')
