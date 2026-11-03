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

"""Element ids: UUIDs, natural-key ids for imports, normalisation."""
from __future__ import annotations

import uuid as _uuid
from typing import Any

# --------------------------------------------------------------------------- #
# Schema: UUID ids + bitmask labels / types
# --------------------------------------------------------------------------- #
def new_id() -> str:
    """A fresh random UUID as canonical lowercase text."""
    return str(_uuid.uuid4())


def bulk_node_id(type_name: str, key: str, value: Any) -> str:
    """The id of the node a file import made for (type, natural key, value): deterministic, so a
    relationship row finds its endpoints without a search and a re-import updates, not duplicates."""
    return stable_id("bulk", "vertex", type_name, key, repr(value))


def stable_id(*parts: Any, namespace: _uuid.UUID = _uuid.NAMESPACE_URL) -> str:
    """Deterministic UUID (v5) from natural-key parts: the same parts always give
    the same id, which makes mergeV/mergeE and re-running imports idempotent."""
    return str(_uuid.uuid5(namespace, "\x1f".join(str(p) for p in parts)))


def norm_id(x: Any) -> str:
    """Canonical UUID text for a str / uuid.UUID; anything else is an error."""
    if isinstance(x, _uuid.UUID):
        return str(x)
    if isinstance(x, str):
        try:
            return str(_uuid.UUID(x))
        except ValueError:
            raise ValueError(f"not a UUID: {x!r}") from None
    raise TypeError(f"ids must be UUIDs (str or uuid.UUID), got {type(x).__name__}")
