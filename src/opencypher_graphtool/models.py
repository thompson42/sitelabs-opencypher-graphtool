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

"""The data model: pydantic models as the one source of truth for schema, tables and validation."""
from __future__ import annotations

import logging
import re
import uuid as _uuid
import warnings
from graph_tool import Graph
from typing import Any
from typing import Iterable
from .errors import ConstraintError, SchemaMigrationError
from .common import CORE_KEYS
from .values import _refuse_maps

log = logging.getLogger(__name__)

try:                                        # pydantic is optional: only needed for Models
    from pydantic import BaseModel as _BaseModel, ConfigDict as _ConfigDict, Field as _PydField
    from pydantic import TypeAdapter as _TypeAdapter, ValidationError as _ValidationError
except ImportError:                         # pragma: no cover
    _BaseModel = None


class MigrationPlan:
    """What Models.plan() found: `statements` to run (safe: CREATE TABLE / ADD column / CREATE
    INDEX), `blocked` (needs a human: type change, missing key column), `warnings` (harmless:
    columns the models no longer mention are left alone)."""

    def __init__(self):
        self.statements: list[str] = []
        self.blocked: list[str] = []
        self.warnings: list[str] = []

    def __bool__(self) -> bool:
        return bool(self.statements)

    def __repr__(self) -> str:
        return (f"<MigrationPlan {len(self.statements)} statement(s), "
                f"{len(self.blocked)} blocked, {len(self.warnings)} warning(s)>")


class _VectorSpec:
    """Marker in `Vector[n]`'s metadata: the field is an embedding of `dim` floats."""
    __slots__ = ("dim",)

    def __init__(self, dim: int):
        self.dim = dim

    def __repr__(self) -> str:
        return f"Vector[{self.dim}]"


class Vector:
    """`Vector[384]`: a list of exactly 384 floats, stored as Cassandra `vector<float, 384>`.

        class Document(VertexModel):
            embedding: Vector[384] | None = VectorField(similarity="cosine")

    Vectors live only in the property store (never in graph-tool's memory). Search them with
    `CALL vector.search(...)`; score them with `vector.cosine(...)` and friends."""

    def __class_getitem__(cls, dim: Any) -> Any:
        if isinstance(dim, bool) or not isinstance(dim, int) or dim < 1:
            raise TypeError(f"Vector[n] needs a positive integer dimension, got {dim!r}")
        from typing import Annotated
        from annotated_types import Len
        return Annotated[list[float], _VectorSpec(dim), Len(dim, dim)]


def _vector_spec(ann: Any, metadata: Iterable[Any] = ()) -> "_VectorSpec | None":
    """The _VectorSpec of a field (pydantic moves a top-level Annotated's metadata into the
    FieldInfo; one inside `Vector[n] | None` stays in the annotation)."""
    import types as _t, typing
    for m in metadata:
        if isinstance(m, _VectorSpec):
            return m
    origin, args = typing.get_origin(ann), typing.get_args(ann)
    if origin is typing.Annotated:
        return next((m for m in args[1:] if isinstance(m, _VectorSpec)), None) or _vector_spec(args[0])
    if origin in (typing.Union, _t.UnionType):
        return next((s for s in (_vector_spec(a) for a in args) if s is not None), None)
    return None


def _cql_type(ann: Any) -> str:
    """Python annotation -> CQL column type. Override per field with Field(cql="...")."""
    import datetime, decimal, enum, types as _t, typing
    origin, args = typing.get_origin(ann), typing.get_args(ann)
    if origin is typing.Annotated:
        spec = next((m for m in args[1:] if isinstance(m, _VectorSpec)), None)
        if spec is not None:
            return f"vector<float, {spec.dim}>"
        return _cql_type(args[0])
    if origin in (typing.Union, _t.UnionType):
        real = [a for a in args if a is not type(None)]
        if len(real) == 1:
            return _cql_type(real[0])
        raise TypeError(f"cannot map the union {ann!r} to one CQL type (use Field(cql=...))")
    if origin is typing.Literal:
        kinds = {type(a) for a in args}
        if kinds == {str}:
            return "text"
        if kinds == {int}:
            return "bigint"
        raise TypeError(f"cannot map {ann!r} to one CQL type (use Field(cql=...))")
    if origin in (list, set, frozenset):
        inner = _cql_type(args[0])
        if inner.startswith(("list<", "set<", "map<")):
            raise TypeError(f"nested collections are not supported ({ann!r})")
        return f"{'list' if origin is list else 'set'}<{inner}>"
    if origin is dict:
        k, v = _cql_type(args[0]), _cql_type(args[1])
        if any(x.startswith(("list<", "set<", "map<")) for x in (k, v)):
            raise TypeError(f"nested collections are not supported ({ann!r})")
        return f"map<{k}, {v}>"
    if isinstance(ann, type):
        if issubclass(ann, enum.Enum):
            vals = {type(m.value) for m in ann}
            return "bigint" if vals == {int} else "text"
        for py, cq in ((bool, "boolean"), (int, "bigint"), (float, "double"), (str, "text"),
                       (datetime.datetime, "timestamp"), (datetime.date, "date"),
                       (datetime.time, "time"), (_uuid.UUID, "uuid"),
                       (decimal.Decimal, "decimal"), (bytes, "blob")):
            if issubclass(ann, py):
                return cq
    raise TypeError(f"cannot map {ann!r} to a CQL type (use Field(cql=...))")


def _to_store(v: Any) -> Any:
    """Validated python value -> what the driver should be given."""
    import enum
    if isinstance(v, enum.Enum):
        return v.value
    if isinstance(v, (list, tuple)):
        return [_to_store(x) for x in v]
    if isinstance(v, (set, frozenset)):
        return {_to_store(x) for x in v}
    if isinstance(v, dict):
        return {_to_store(k): _to_store(x) for k, x in v.items()}
    return v


def _norm_cql(t: str) -> str:
    t = re.sub(r"\s+", "", str(t).lower()).replace("varchar", "text")
    return re.sub(r"frozen<(.*)>", r"\1", t)


def _index_targets(table_meta: Any) -> set:
    return set(_index_details(table_meta))


def _index_details(table_meta: Any) -> dict:
    """{indexed column: (index name, its options)} from the driver's table metadata."""
    out = {}
    for name, idx in (getattr(table_meta, "indexes", None) or {}).items():
        opts = dict(idx.index_options or {})
        tgt = (opts.get("target", "") or "").strip()
        out[re.sub(r"^(values|keys|entries)\((.*)\)$", r"\2", tgt).strip('"')] = (name, opts)
    return out


def _is_sai(options: dict) -> bool:
    cls = str(options.get("class_name", "")).lower()
    return cls == "sai" or "storageattachedindex" in cls


class _FieldInfo:
    __slots__ = ("name", "annotation", "cql", "index", "required", "dim", "similarity", "_adapter", "_field")

    def __init__(self, name: str, field: Any):
        self.name, self._field, self._adapter = name, field, None
        self.annotation = field.annotation
        extra = field.json_schema_extra if isinstance(field.json_schema_extra, dict) else {}
        self.index = bool(extra.get("index"))
        spec = _vector_spec(field.annotation, field.metadata)
        self.dim = spec.dim if spec is not None else None
        self.similarity = extra.get("vector_similarity")
        if self.similarity is not None and self.dim is None:
            raise TypeError(f"{name}: VectorField needs a Vector[n] annotation")
        if self.dim is not None and self.index and self.similarity is None:
            self.similarity = "cosine"
        if self.dim is not None:
            self.cql = extra.get("cql") or f"vector<float, {self.dim}>"
        else:
            self.cql = extra.get("cql") or _cql_type(field.annotation)
        self.required = field.is_required()

    def adapter(self):
        if self._adapter is None:
            from typing import Annotated
            if self._field.metadata:
                ann = Annotated[(self.annotation, *self._field.metadata)]
            else:
                ann = self.annotation
            self._adapter = _TypeAdapter(ann)
        return self._adapter


if _BaseModel is not None:
    def Field(*args: Any, index: bool = False, cql: str | None = None, **kw: Any) -> Any:
        """pydantic.Field plus two graph options: `index=True` creates a Cassandra storage-attached
        index (SAI), so the property can be searched / pushed down; `cql="..."` overrides the
        column type."""
        extra = dict(kw.pop("json_schema_extra", None) or {})
        if index:
            extra["index"] = True
        if cql:
            extra["cql"] = cql
        return _PydField(*args, json_schema_extra=extra or None, **kw)

    def VectorField(*args: Any, similarity: str = "cosine", index: bool = True, **kw: Any) -> Any:
        """A `Vector[n]` field with a vector index, so `CALL vector.search(...)` can find nearest
        neighbours in Cassandra. `similarity`: "cosine" (default), "dot_product" (for unit-length
        vectors) or "euclidean"; it is fixed once the index exists. Defaults to None (optional)."""
        if similarity not in VECTOR_SIMILARITIES:
            raise ValueError(f"similarity must be one of {sorted(VECTOR_SIMILARITIES)}, got {similarity!r}")
        if not args and "default" not in kw and "default_factory" not in kw:
            kw["default"] = None
        extra = dict(kw.pop("json_schema_extra", None) or {})
        extra["vector_similarity"] = similarity
        return Field(*args, index=index, json_schema_extra=extra, **kw)

    class _GraphModel(_BaseModel):
        model_config = _ConfigDict(extra="forbid")

        def __init_subclass__(cls, **kw: Any) -> None:       # accept class kwargs: type=, labels=, table=
            super().__init_subclass__()

        @classmethod
        def __pydantic_init_subclass__(cls, **kw: Any) -> None:
            super().__pydantic_init_subclass__()
            cls.__gt_kw__ = kw

    class VertexModel(_GraphModel):
        """Describes one vertex type = one Cassandra table. Configure with an inner `class Meta:`
        (type, labels, table) or class keywords: `class Person(VertexModel, table="people")`."""

    class EdgeModel(_GraphModel):
        """Describes one edge type = one Cassandra table (label = type)."""
else:                                                         # pragma: no cover
    def Field(*args: Any, **kw: Any) -> Any:
        raise ImportError("pydantic is required for Models (pip install pydantic)")

    VectorField = Field

    class VertexModel:                                        # type: ignore[no-redef]
        def __init_subclass__(cls, **kw):
            raise ImportError("pydantic is required for Models (pip install pydantic)")

    class EdgeModel(VertexModel):                             # type: ignore[no-redef]
        pass


def _snake(name: str) -> str:
    """Default table name: ActedIn -> acted_in, ACTED_IN -> acted_in, HTTPServer -> http_server."""
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])", "_", name).lower()


class _ModelInfo:
    def __init__(self, cls: type):
        self.cls = cls
        self.kind = "vertex" if issubclass(cls, VertexModel) else "edge"
        own = cls.__dict__.get("Meta")
        kw = cls.__dict__.get("__gt_kw__", {})

        def pick(name, default):
            return kw.get(name, getattr(own, name, default))
        self.type = pick("type", cls.__name__)
        self.labels = list(pick("labels", []))
        self.table = pick("table", _snake(self.type))
        # Optional, descriptive only (not enforced): a vertex type's natural key, and the vertex
        # types an edge type connects. They shape the example import files (example_imports.py).
        self.key = pick("key", None)
        self.source, self.target = pick("source", None), pick("target", None)
        self.fields: dict[str, _FieldInfo] = {}
        for n, f in cls.model_fields.items():
            if n in CORE_KEYS:
                raise ValueError(f"{cls.__name__}.{n}: {n!r} is reserved (id/label/type are served by graph-tool)")
            self.fields[n] = _FieldInfo(n, f)
        if self.kind == "edge" and self.labels:
            raise ValueError(f"{cls.__name__}: an edge has exactly one type and no labels")
        if self.key is not None and self.key not in self.fields:
            raise ValueError(f"{cls.__name__}: Meta.key {self.key!r} is not one of its fields")
        if self.kind == "vertex" and (self.source or self.target):
            raise ValueError(f"{cls.__name__}: Meta.source / Meta.target are for edge types")


class Models:
    """Pydantic models as the single source of truth.

        class Person(VertexModel):
            name: str = Field(index=True)          # -> Cassandra secondary index
            age: int | None = None
        class Knows(EdgeModel):
            since: int | None = None

        cy = opencypher(graph, store, models=Models([Person, Knows]))

    What it drives: the Schema (types and labels), table names, CREATE TABLE / ALTER TABLE ADD /
    CREATE INDEX (run automatically; see plan()/apply()), value validation when a property is set,
    and strict whole-row validation (required fields, validators, defaults) at commit.
    """

    def __init__(self, models: Iterable[type], *, bits: int = 64, auto_register: bool = False):
        if _BaseModel is None:
            raise ImportError("pydantic is required for Models (pip install pydantic)")
        self.bits, self.auto_register = bits, auto_register
        self._by: dict[tuple, _ModelInfo] = {}
        tables: dict[str, str] = {}
        for cls in models:
            if not (isinstance(cls, type) and issubclass(cls, (VertexModel, EdgeModel))):
                raise TypeError(f"{cls!r} is not a VertexModel / EdgeModel subclass")
            info = _ModelInfo(cls)
            key = (info.kind, info.type)
            if key in self._by:
                raise ValueError(f"two models for {info.kind} type {info.type!r}")
            if info.table in tables:
                raise ValueError(f"table {info.table!r} is used by both {tables[info.table]} and {cls.__name__}")
            tables[info.table] = cls.__name__
            self._by[key] = info
        vertex_types = set(self.type_names("vertex"))
        for info in self._by.values():
            for end in (info.source, info.target):
                if end is not None and end not in vertex_types:
                    raise ValueError(f"{info.cls.__name__}: Meta.source / target {end!r} is not a vertex type")

    # -- lookups ---------------------------------------------------------------------- #
    def type_names(self, kind: str) -> list[str]:
        return [t for (k, t) in self._by if k == kind]

    def model(self, kind: str, type_name: str) -> type:
        return self._by[(kind, type_name)].cls

    def info(self, kind: str, type_name: str) -> "_ModelInfo":
        return self._by[(kind, type_name)]

    def table_namer(self, kind: str, type_name: str) -> str:
        info = self._by.get((kind, type_name))
        if info is None:
            raise KeyError(f"no model for {kind} type {type_name!r}")
        return info.table

    def pick_type(self, labels: Iterable[str]) -> "str | None":
        """The vertex type for a set of labels: the first one that has a model."""
        for l in labels:
            if ("vertex", l) in self._by:
                return l
        return None

    # -- schema ----------------------------------------------------------------------- #
    def schema(self, graph: Graph | None = None, **kw: Any) -> Schema:
        """A Schema for these models. If `graph` already carries a saved Schema its name->bit map is
        reused and only NEW names get free bits, so reordering/adding models never moves a bit."""
        base = Schema.from_graph(graph) if graph is not None else None
        sc = base if base is not None else Schema(bits=self.bits, auto_register=self.auto_register, **kw)
        self.register_into(sc)
        return sc

    def register_into(self, schema: Schema) -> None:
        vt = self.type_names("vertex")
        labels = list(vt)
        for i in self._by.values():
            labels += [l for l in i.labels if l not in labels]
        for kind, what, names in (("vertex", "type", vt), ("vertex", "label", labels),
                                  ("edge", "type", self.type_names("edge"))):
            for n in names:
                schema.reg(kind, what).register(n)

    # -- DDL -------------------------------------------------------------------------- #
    def _table_stmts(self, ks: str, info: _ModelInfo, id_col: str, id_type: str) -> list[str]:
        cols = ", ".join(f'"{n}" {f.cql}' for n, f in info.fields.items())
        out = [f'CREATE TABLE IF NOT EXISTS "{ks}"."{info.table}" ("{id_col}" {id_type} PRIMARY KEY'
               + (f", {cols}" if cols else "") + ")"]
        out += [self._index_stmt(ks, info, f) for f in info.fields.values() if f.index]
        return out

    @staticmethod
    def _index_stmt(ks: str, info: _ModelInfo, f: _FieldInfo) -> str:
        """A storage-attached index (SAI); for a vector field, with its similarity function."""
        if f.cql.startswith("map<"):
            raise ValueError(f"{info.cls.__name__}.{f.name}: indexing a map is not supported")
        target = f'VALUES("{f.name}")' if f.cql.startswith(("list<", "set<")) else f'"{f.name}"'
        stmt = (f'CREATE INDEX IF NOT EXISTS "{info.table}_{f.name}_idx" ON "{ks}"."{info.table}" ({target}) '
                f"USING 'sai'")
        if f.dim is not None:
            stmt += f" WITH OPTIONS = {{'similarity_function': '{VECTOR_SIMILARITIES[f.similarity]}'}}"
        return stmt

    def ddl(self, keyspace: str, *, id_type: str = "uuid", id_column: str = "id") -> list[str]:
        """The full CREATE TABLE / CREATE INDEX script for a fresh keyspace."""
        out: list[str] = []
        for info in self._by.values():
            out += self._table_stmts(keyspace, info, id_column, id_type)
        return out

    def plan(self, store: Any, reverse: bool = False) -> MigrationPlan:
        """Compare the models with the live tables: new tables, new columns and missing indexes
        are SAFE statements; a changed column type or missing key column is `blocked`; columns
        the models do not mention are only reported."""
        p = MigrationPlan()
        ks, id_col = store.keyspace, store.id_column
        meta_ks = store.session.cluster.metadata.keyspaces.get(ks)
        if meta_ks is None:
            raise SchemaMigrationError(f"keyspace {ks!r} does not exist; create it first "
                                       "(replication settings are your decision)")
        tables = meta_ks.tables
        for info in self._by.values():
            tm = tables.get(info.table)
            if tm is None:
                p.statements += self._table_stmts(ks, info, id_col, store.id_type)
                continue
            if id_col not in tm.columns:
                p.blocked.append(f'table {info.table!r} has no "{id_col}" column')
                continue
            have_idx = _index_details(tm)
            for n, f in info.fields.items():
                col = tm.columns.get(n)
                if col is None:
                    p.statements.append(f'ALTER TABLE "{ks}"."{info.table}" ADD "{n}" {f.cql}')
                    if f.index:
                        p.statements.append(self._index_stmt(ks, info, f))
                elif _norm_cql(getattr(col, "cql_type", "")) != _norm_cql(f.cql):
                    p.blocked.append(f"{info.table}.{n}: column is {col.cql_type}, model says {f.cql} "
                                     "(Cassandra cannot change a column type in place)")
                elif f.index and n not in have_idx:
                    p.statements.append(self._index_stmt(ks, info, f))
                elif f.index:
                    self._check_index(p, ks, info, f, *have_idx[n])
            extra = [c for c in tm.columns if c != id_col and c not in info.fields]
            if extra:
                p.warnings.append(f"{info.table}: columns not in {info.cls.__name__} are left untouched: {extra}")
        if reverse and store.reverse_table not in tables:
            p.statements.append(store.reverse_table_ddl())
        return p

    @staticmethod
    def _check_index(p: MigrationPlan, ks: str, info: _ModelInfo, f: _FieldInfo, name: str, opts: dict) -> None:
        """An index already exists on an indexed field: is it the right kind?"""
        if f.dim is not None:
            have = str(opts.get("similarity_function", "COSINE")).upper()
            want = VECTOR_SIMILARITIES[f.similarity]
            if have != want:
                p.blocked.append(
                    f"{info.table}.{f.name}: the vector index {name!r} uses {have}, the model says {want} "
                    f'(drop it by hand: DROP INDEX "{ks}"."{name}", then run the migration again)')
        elif not _is_sai(opts):
            p.warnings.append(
                f"{info.table}.{f.name}: index {name!r} is an older secondary index, not SAI. Queries work, "
                f'but it can\'t filter a vector search; to rebuild it: DROP INDEX "{ks}"."{name}", then run '
                "the migration again")

    def apply(self, store: Any, *, reverse: bool = False, allow_blocked: bool = False) -> MigrationPlan:
        """Run plan(): create tables, ADD columns, create indexes. Raises SchemaMigrationError
        (changing nothing) if something is blocked, unless allow_blocked=True."""
        p = self.plan(store, reverse=reverse)
        if p.blocked and not allow_blocked:
            log.error("schema migration blocked: %s", "; ".join(p.blocked))
            raise SchemaMigrationError("cannot migrate automatically:\n  " + "\n  ".join(p.blocked))
        if p.statements:
            log.info("schema migration: %s statement(s)", len(p.statements))
        for st in p.statements:
            log.debug("schema: %s", st)
            store.session.execute(st)
        if p.statements:
            refresh = getattr(store.session.cluster, "refresh_schema_metadata", None)
            if refresh is not None:
                refresh()
        for w in p.warnings:
            warnings.warn(w, RuntimeWarning, stacklevel=2)
        return p

    # -- validation ------------------------------------------------------------------- #
    def coerce_props(self, kind: str, type_name: str, props: dict) -> dict:
        """Type-check / convert each value against its field (when a property is SET)."""
        info = self._by.get((kind, type_name))
        if info is None:
            raise KeyError(f"no model for {kind} type {type_name!r}")
        _refuse_maps(props, lambda k: k in info.fields and info.fields[k].cql.startswith("map<"))
        out = {}
        for k, v in props.items():
            f = info.fields.get(k)
            if f is None:
                raise KeyError(f"{info.cls.__name__} has no field {k!r}")
            if v is None:
                out[k] = None
                continue
            try:
                out[k] = _to_store(f.adapter().validate_python(v))
            except _ValidationError as ex:
                probs = "; ".join(e["msg"] for e in ex.errors())
                raise ValueError(f"{info.cls.__name__}.{k}: {probs}") from None
        return out

    def validate_row(self, kind: str, type_name: str, row: dict, label: str = "") -> dict:
        """Validate a WHOLE row against its model (required fields, validators); returns the
        final stored form of every field (defaults filled in)."""
        info = self._by[(kind, type_name)]
        data = {k: v for k, v in row.items() if k in info.fields}
        try:
            obj = info.cls.model_validate(data)
        except _ValidationError as ex:
            probs = "; ".join(f"{'.'.join(map(str, e['loc'])) or '<row>'}: {e['msg']}" for e in ex.errors())
            raise ConstraintError(f"{label or info.cls.__name__}: {probs}") from None
        return {n: _to_store(getattr(obj, n)) for n in info.fields}


def _link() -> None:
    """Bind the names this module uses from modules loaded after it (they import this one).
    Called once by the package __init__, after every module is loaded."""
    global Schema, VECTOR_SIMILARITIES
    from .schema import Schema
    from .graph_functions import VECTOR_SIMILARITIES
