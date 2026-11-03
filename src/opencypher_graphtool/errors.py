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

"""The engine's exceptions."""
from __future__ import annotations


# --------------------------------------------------------------------------- #
# Concurrency: one fair readers-writer lock per graph
# --------------------------------------------------------------------------- #
class LockError(RuntimeError):
    """Base class for locking problems."""


class LockUpgradeError(LockError):
    """A write was attempted while the same thread holds only a read lock."""


class LockTimeout(LockError, TimeoutError):
    """The lock was not granted within the requested time."""




# =========================================================================== #
# openCypher: lexer
# =========================================================================== #
class OpenCypherError(Exception):
    """Base class for openCypher syntax / semantic / runtime errors."""


class OpenCypherSyntaxError(OpenCypherError):
    pass


class QueryTimeout(OpenCypherError, TimeoutError):
    """The query ran longer than cy.timeout (a write query is rolled back)."""


class ConsistencyError(OpenCypherError):
    """A write failed AND the compensating rollback could not finish (e.g. the store went down
    mid-flush and is still down). graph-tool has been rolled back; some store rows may differ.
    The pending corrections are kept in `store.unresolved`; call `cy.repair()` once the store is
    reachable again. `.original` is the exception that caused the rollback."""

    def __init__(self, msg: str, original: BaseException | None = None, unresolved: list | None = None):
        super().__init__(msg)
        self.original, self.unresolved = original, list(unresolved or [])


# =========================================================================== #
# openCypher: public API
# =========================================================================== #
# =========================================================================== #
# Pydantic models: ONE source of truth for schema, tables and validation
# =========================================================================== #
class ConstraintError(OpenCypherError):
    """A row violates its pydantic model (missing required field, failed validator, ...).
    Raised at the commit point, so the whole query is rolled back."""


class SchemaMigrationError(OpenCypherError):
    """The tables cannot be brought in line with the models automatically (e.g. a column would
    need a different type). Nothing was changed."""
