"""hydrowf lego models (design sketch, pydantic v2).

Two lego types live in one registry:
- ToolLego: compute (image + args + typed ports)
- DataLego: a dataset (external read-only, managed by the platform, or a run output)

A Port of type file/directory names the DataLego id it carries, so the canvas only connects
matching ports. Storage specs are generic (iceberg / stac / external): the platform knows kinds
of data, never domains.

Not modelled here on purpose: who may create what (backend authorization: tool-authors /
data-authors create managed data and deploy tools; anyone may add external data).

"""
from __future__ import annotations

import re
from enum import Enum
from graphlib import CycleError, TopologicalSorter
from typing import Annotated, Literal, Union

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, TypeAdapter, model_validator

LegoId = Annotated[str, Field(pattern=r"^[a-z0-9][a-z0-9.\-]*$", max_length=63)]
SemVer = Annotated[str, Field(pattern=r"^\d+\.\d+\.\d+$")]


class Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Visibility(str, Enum):
    OWNER = "owner"
    GROUP = "group"
    PUBLIC = "public"


class LegoBase(Strict):
    id: LegoId
    version: SemVer                       # immutable: a change is a new version
    title: str
    description: str = ""
    owner: str = ""                       # set by the backend from the Keycloak user, never by the form
    visibility: Visibility = Visibility.OWNER


# ------------------------------------------------------------------ data legos

class ColumnType(str, Enum):
    STRING = "string"
    INT = "int"
    LONG = "long"
    FLOAT = "float"
    DOUBLE = "double"
    BOOLEAN = "boolean"
    DATE = "date"
    TIMESTAMP = "timestamp"
    BINARY = "binary"
    GEOMETRY = "geometry"                 # stored as WKB; needs a CRS


class Column(Strict):
    name: str
    type: ColumnType
    required: bool = False
    crs: int | None = None                # EPSG code, geometry columns only


class SchemaRef(Strict):
    ref: str                              # a registered community schema, e.g. "teehr.secondary_timeseries@0.8"


# Iceberg partition transforms -> column types they accept (https://iceberg.apache.org/spec/#partition-transforms).
# Geometry is never a partition source: Iceberg v2 has no spatial transform, so spatial layout
# comes from bbox columns plus the sort order.
_T = ColumnType
PARTITION_SOURCE_TYPES: dict[str, frozenset[ColumnType]] = {
    "identity": frozenset(_T) - {_T.GEOMETRY},
    "year": frozenset({_T.DATE, _T.TIMESTAMP}),
    "month": frozenset({_T.DATE, _T.TIMESTAMP}),
    "day": frozenset({_T.DATE, _T.TIMESTAMP}),
    "hour": frozenset({_T.TIMESTAMP}),
    "bucket": frozenset({_T.INT, _T.LONG, _T.DATE, _T.TIMESTAMP, _T.STRING, _T.BINARY}),
    "truncate": frozenset({_T.INT, _T.LONG, _T.STRING, _T.BINARY}),
}


class Partition(Strict):
    """One Iceberg partition field (the transforms pyiceberg 0.12 provides)."""
    transform: Literal["identity", "year", "month", "day", "hour", "bucket", "truncate"]
    column: str
    width: int | None = None              # bucket count / truncate width


class TableSpec(Strict):
    columns: list[Column] | SchemaRef
    partition: list[Partition] = []
    sort: list[str] = []

    @model_validator(mode="after")
    def check_layout(self) -> TableSpec:
        """Reject a layout Iceberg can't build, before any table is created.

        With a SchemaRef the columns aren't known here; those specs are checked when the ref
        is resolved, so return early.
        """
        if isinstance(self.columns, SchemaRef):
            return self
        types = {c.name: c.type for c in self.columns}

        for p in self.partition:
            if p.column not in types:
                raise ValueError(f"partition column {p.column!r} is not a column of this table")
            col_type = types[p.column]
            allowed = PARTITION_SOURCE_TYPES[p.transform]
            if col_type not in allowed:
                names = ", ".join(sorted(t.value for t in allowed))
                raise ValueError(f"{p.transform}({p.column}) needs a column of type {names}; "
                                 f"{p.column!r} is {col_type.value}")
            if p.transform in ("bucket", "truncate"):
                if p.width is None or p.width <= 0:
                    raise ValueError(f"{p.transform}({p.column}) needs a positive width")
            elif p.width is not None:
                raise ValueError(f"{p.transform}({p.column}) takes no width")

        for name in self.sort:
            if name not in types:
                raise ValueError(f"sort column {name!r} is not a column of this table")

        for c in self.columns:
            if c.type == ColumnType.GEOMETRY and c.crs is None:
                raise ValueError(f"geometry column {c.name!r} needs a crs (EPSG code)")
            if c.type != ColumnType.GEOMETRY and c.crs is not None:
                raise ValueError(f"column {c.name!r} has a crs but is not a geometry column")
        return self


class IcebergStorage(Strict):
    driver: Literal["iceberg"]
    tables: dict[str, TableSpec]


class StacStorage(Strict):
    driver: Literal["stac"]
    format: Literal["cog", "zarr", "geoparquet", "parquet", "netcdf", "other"]
    extensions: list[str] = []            # STAC extensions, e.g. raster, projection, datacube
    item_from: str | None = "manifest.json"   # per-file metadata the producing tool writes
    properties: dict[str, str] = {}       # STAC property -> manifest / run field


class ExternalStorage(Strict):
    driver: Literal["external"]
    url: Annotated[str, Field(pattern=r"^(s3|https)://")]   # no file://, no internal hosts
    format: Literal["zarr", "netcdf", "parquet", "cog", "api", "other"]
    access: Literal["anonymous"] = "anonymous"              # v1: no stored credentials


Storage = Annotated[Union[IcebergStorage, StacStorage, ExternalStorage], Field(discriminator="driver")]


class DataLego(LegoBase):
    kind: Literal["data"] = "data"
    data_kind: Literal["external", "managed", "output"]
    storage: Storage

    @model_validator(mode="after")
    def external_means_external_storage(self) -> DataLego:
        if (self.data_kind == "external") != isinstance(self.storage, ExternalStorage):
            raise ValueError("data_kind 'external' goes with driver 'external', and only with it")
        return self


# ------------------------------------------------------------------ tool legos

SCALAR_TYPES = ("string", "int", "float", "boolean", "enum")
DATA_TYPES = ("file", "directory", "dataset")
Scalar = Union[str, int, float, bool]

# Python types a constant may have for each scalar port type (bool is an int in Python: keep it apart).
VALUE_TYPES = {"string": (str,), "enum": (str,), "int": (int,), "float": (int, float), "boolean": (bool,)}


def value_problem(port_type: str, symbols: list[str], value: Scalar, pattern: str | None = None) -> str | None:
    """Why `value` can't be a value of a port of `port_type`, or None if it can. `pattern` must match
    the whole of a string value."""
    allowed = VALUE_TYPES.get(port_type)
    if allowed is None:
        return f"a {port_type} port can't take a constant"
    if isinstance(value, bool) and bool not in allowed:
        return f"{value!r} is a boolean, the port is {port_type}"
    if not isinstance(value, allowed):
        return f"{value!r} is not a {port_type}"
    if port_type == "enum" and value not in symbols:
        return f"{value!r} is not one of {symbols}"
    if pattern is not None and not re.fullmatch(pattern, value, flags=re.ASCII):   # \d is 0-9, as in JSON Schema
        return f"{value!r} does not match {pattern}"
    return None


def _container_path(absolute: bool):
    """Paths inside the container: relative ones sit under the tool's workdir. Never "..", so a
    staged input can't land outside it (pydantic's regex engine has no look-ahead, hence a function)."""
    def check(path: str) -> str:
        if not path or path.startswith("/") != absolute:
            raise ValueError(f"{path!r}: must be {'an absolute' if absolute else 'a relative'} path")
        if ".." in path.split("/"):
            raise ValueError(f"{path!r}: '..' is not allowed")
        return path
    return AfterValidator(check)


RelPath = Annotated[str, _container_path(absolute=False)]
AbsPath = Annotated[str, _container_path(absolute=True)]

PLACEHOLDER = re.compile(r"\{([a-z_][a-z0-9_]*)\}")
# Placeholders the runtime fills, next to the input names. {run} is this run's id as the runtime
# names it (Argo: the workflow name). It is only known when the run starts, so it can be used in
# args and env, not in paths (which are fixed before the run has a name).
RUNTIME_NAMES = ("workdir", "cpu", "run")


def fill(text: str, values: dict[str, Scalar]) -> str:
    """Replace each {name} in `text` with values[name] (booleans as true/false, 4.0 as 4)."""
    def one(match: re.Match) -> str:
        value = values[match.group(1)]
        if isinstance(value, bool):
            return str(value).lower()
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
        return str(value)
    return PLACEHOLDER.sub(one, text)


class Port(Strict):
    """An input or output of a tool or pipeline.

    file/directory: bytes the runtime moves through S3 (Argo artifacts); `path` says where they sit
    under the tool's workdir (inputs default to inputs/<name>; outputs must say).
    dataset: a reference to a stored dataset (Iceberg table, STAC collection, external URL) that
    the tool reads in place; nothing is copied.
    """
    name: Annotated[str, Field(pattern=r"^[a-z_][a-z0-9_]*$")]
    type: Literal["string", "int", "float", "boolean", "enum", "file", "directory", "dataset"]
    data: LegoId | None = None            # file/directory/dataset: the DataLego id this port carries
    symbols: list[str] = []               # enum only
    # string only: a regex the whole value must match. It is also the OGC schema's pattern, so keep to
    # what Python and JSON Schema (ECMA-262) share: no named groups, inline flags or \Z
    pattern: str | None = None
    optional: bool = False
    default: Scalar | None = None         # scalars: used when a step leaves the input unconnected
    path: RelPath | None = None           # file/directory: may use {input} placeholders
    description: str = ""

    @model_validator(mode="after")
    def check_port(self) -> Port:
        if self.type in DATA_TYPES and not self.data:
            raise ValueError(f"port {self.name}: a {self.type} port needs `data` (a data lego id)")
        if self.type not in DATA_TYPES and self.data:
            raise ValueError(f"port {self.name}: only file/directory/dataset ports carry `data`")
        if (self.type == "enum") != bool(self.symbols):
            raise ValueError(f"port {self.name}: `symbols` goes with type enum, and only with it")
        if self.path is not None and self.type not in ("file", "directory"):
            raise ValueError(f"port {self.name}: only file/directory ports have a `path`")
        if self.pattern is not None:
            if self.type != "string":
                raise ValueError(f"port {self.name}: only string ports have a `pattern`")
            try:
                re.compile(self.pattern, flags=re.ASCII)
            except re.error as err:
                raise ValueError(f"port {self.name}: pattern {self.pattern!r}: {err}") from err
        if self.default is not None:
            problem = value_problem(self.type, self.symbols, self.default, self.pattern)
            if problem:
                raise ValueError(f"port {self.name}: default {problem}")
        return self


class ArgGroup(Strict):
    """Words added only when input `when` is set (not missing, not None, not false)."""
    when: str
    args: Annotated[list[str], Field(min_length=1)]


class ToolLego(LegoBase):
    """A container tool. The runtime stages file/directory inputs at their paths under `workdir`,
    writes `files`, runs `command` (or the image's ENTRYPOINT) with `argv(...)`, then collects
    file/directory outputs from their paths."""
    kind: Literal["tool"] = "tool"
    image: Annotated[str, Field(pattern=r"^[^:@\s]+(:[\w.\-]+|@sha256:[0-9a-f]{64})$")]  # pinned
    command: list[str] | None = None      # fixed words; None: the image's ENTRYPOINT
    args: list[Union[str, ArgGroup]] = []  # "{name}" is an input's value, {workdir}, {cpu} or {run}
    inputs: list[Port] = []
    outputs: list[Port] = []
    workdir: AbsPath = "/work"            # also where relative paths start
    env: dict[str, str] = {}              # values may use placeholders
    files: dict[RelPath, str] = {}        # extra files under workdir (path -> content), e.g. markers
    network: bool = False                 # the tool reads from the internet (public buckets, APIs)
    cpu: float = 1
    ram_mb: int = 1024
    runs_on: list[Literal["argo", "hpc"]] = ["argo", "hpc"]

    @model_validator(mode="after")
    def check_tool(self) -> ToolLego:
        for side, ports in (("input", self.inputs), ("output", self.outputs)):
            names = [p.name for p in ports]
            if len(names) != len(set(names)):
                raise ValueError(f"{side} names must be unique")
        inputs = {p.name: p for p in self.inputs}
        for name in RUNTIME_NAMES:
            if name in inputs:
                raise ValueError(f"input {name!r}: the name is reserved for the runtime")
        for port in self.outputs:
            if port.type in ("file", "directory") and port.path is None:
                raise ValueError(f"output {port.name}: a {port.type} output needs a `path`")
        for word in self.command or []:
            if PLACEHOLDER.search(word):
                raise ValueError(f"command {word!r}: placeholders go in `args`, not `command`")

        def check(text: str, where: str, group: str | None = None, in_path: bool = False) -> None:
            for name in PLACEHOLDER.findall(text):
                if name == "run" and in_path:
                    raise ValueError(f"{where} {text!r}: {{run}} is only known when the run starts; "
                                     "use it in args or env, not in paths")
                if name in RUNTIME_NAMES:
                    continue
                port = inputs.get(name)
                if port is None:
                    raise ValueError(f"{where} {text!r}: {{{name}}} is not an input")
                if port.optional and port.default is None and name != group:
                    raise ValueError(f"{where} {text!r}: {{{name}}} is optional; "
                                     f"put it in a group with when: {name}")

        for arg in self.args:
            if isinstance(arg, ArgGroup):
                if arg.when not in inputs:
                    raise ValueError(f"args group: when {arg.when!r} is not an input")
                for word in arg.args:
                    check(word, "args", group=arg.when)
            else:
                check(arg, "args")
        for key, value in self.env.items():
            check(value, f"env {key}")
        for port in self.inputs + self.outputs:
            if port.path:
                check(port.path, f"{port.name} path", in_path=True)
        for path in self.files:
            check(path, "files", in_path=True)
        return self

    def argv(self, values: dict[str, Scalar | None]) -> list[str]:
        """The container's arguments for one run.

        `values` has every input name (defaults already applied; unset optional inputs as None;
        file/directory inputs as their absolute paths) plus "workdir", "cpu" and "run". Plain words are
        filled with `fill`; an ArgGroup's words are added (filled) only when values[group.when]
        is neither None nor False.
        """
        argv: list[str] = []
        for arg in self.args:
            if isinstance(arg, ArgGroup):
                if values.get(arg.when) not in (None, False):     # `is not`: 0 and "" still count as set
                    argv += [fill(word, values) for word in arg.args]
            else:
                argv.append(fill(arg, values))
        return argv


# ------------------------------------------------------------------ pipelines

class LegoRef(Strict):
    """A pinned reference to a registered lego: pipelines never float to 'latest'."""
    id: LegoId
    version: SemVer

    def key(self) -> tuple[str, str]:
        return (self.id, self.version)


# Where a step input gets its value. Exactly one of these shapes (extra="forbid" keeps them apart).
class Value(Strict):
    value: str | int | float | bool       # a constant, fixed in the pipeline


class PipelineInput(Strict):
    input: str                            # a pipeline input, filled per run (job / OGC execute)


class Link(Strict):
    step: str                             # another step's output: the edge of the graph
    output: str


class DataSource(Strict):
    data: LegoRef                         # a registered data lego (external or managed)


Source = Union[Value, PipelineInput, Link, DataSource]
StepName = Annotated[str, Field(pattern=r"^[a-z][a-z0-9-]*$", max_length=40)]   # also the Argo task name


class Step(Strict):
    name: StepName
    tool: LegoRef
    inputs: dict[str, Source] = {}        # tool input port name -> where its value comes from


class Pipeline(LegoBase):
    """Steps wired together. Checks here only need the pipeline itself; checks that need the
    tools and data legos (ports exist, types match) are in hydrowf_core.registry."""
    kind: Literal["pipeline"] = "pipeline"
    inputs: list[Port] = []               # what a run must provide
    outputs: dict[str, Link] = {}         # pipeline output name -> a step output
    steps: Annotated[list[Step], Field(min_length=1)]

    @model_validator(mode="after")
    def check_wiring(self) -> Pipeline:
        names = [s.name for s in self.steps]
        if len(set(names)) != len(names):
            raise ValueError(f"step names must be unique: {sorted(n for n in set(names) if names.count(n) > 1)}")
        declared = {p.name for p in self.inputs}
        for s in self.steps:
            for port, src in s.inputs.items():
                if isinstance(src, PipelineInput) and src.input not in declared:
                    raise ValueError(f"step {s.name}.{port}: pipeline input {src.input!r} is not declared")
                if isinstance(src, Link) and src.step not in names:
                    raise ValueError(f"step {s.name}.{port}: links to unknown step {src.step!r}")
        for out, link in self.outputs.items():
            if link.step not in names:
                raise ValueError(f"pipeline output {out}: links to unknown step {link.step!r}")
        self.order()                      # raises on a cycle
        return self

    def order(self) -> list[str]:
        """Step names so that every step comes after the steps it links from.

        Raises ValueError on a cycle (including a step linking to itself). The Argo adapter
        uses the same order for task dependencies; the HPC adapter for the CWL step order.
        """
        graph = TopologicalSorter()
        for s in self.steps:                              # add every step, even unlinked ones
            graph.add(s.name, *(src.step for src in s.inputs.values() if isinstance(src, Link)))
        try:
            return list(graph.static_order())
        except CycleError as err:
            raise ValueError(f"steps form a cycle: {' -> '.join(err.args[1])}") from None


Lego = Annotated[Union[ToolLego, DataLego, Pipeline], Field(discriminator="kind")]
LEGOS = TypeAdapter(list[Lego])
