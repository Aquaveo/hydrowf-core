"""hydrowf lego models (design sketch, pydantic v2).

Two lego types live in one registry:
- ToolLego: compute (image + command + typed ports)
- DataLego: a dataset (external read-only, managed by the platform, or a run output)

A Port of type file/directory names the DataLego id it carries, so the canvas only connects
matching ports. Storage specs are generic (iceberg / stac / external): the platform knows kinds
of data, never domains.

Not modelled here on purpose: who may create what (backend authorization: tool-authors /
data-authors create managed data and deploy tools; anyone may add external data).

"""
from __future__ import annotations

from enum import Enum
from graphlib import CycleError, TopologicalSorter
from typing import Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator

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


class Port(Strict):
    """An input or output of a tool or pipeline.

    file/directory: bytes the runtime moves through S3 (Argo artifacts).
    dataset: a reference to a stored dataset (Iceberg table, STAC collection, external URL) that
    the tool reads in place; nothing is copied.
    """
    name: Annotated[str, Field(pattern=r"^[a-z_][a-z0-9_]*$")]
    type: Literal["string", "int", "float", "boolean", "enum", "file", "directory", "dataset"]
    data: LegoId | None = None            # file/directory/dataset: the DataLego id this port carries
    symbols: list[str] = []               # enum only
    optional: bool = False
    description: str = ""

    @model_validator(mode="after")
    def check_port(self) -> Port:
        if self.type in DATA_TYPES and not self.data:
            raise ValueError(f"port {self.name}: a {self.type} port needs `data` (a data lego id)")
        if self.type not in DATA_TYPES and self.data:
            raise ValueError(f"port {self.name}: only file/directory/dataset ports carry `data`")
        if (self.type == "enum") != bool(self.symbols):
            raise ValueError(f"port {self.name}: `symbols` goes with type enum, and only with it")
        return self


class ToolLego(LegoBase):
    kind: Literal["tool"] = "tool"
    image: Annotated[str, Field(pattern=r"^[^:@\s]+(:[\w.\-]+|@sha256:[0-9a-f]{64})$")]  # pinned
    command: list[str]                    # words; "{name}" is replaced by the input's value
    inputs: list[Port] = []
    outputs: list[Port] = []
    cpu: float = 1
    ram_mb: int = 1024
    runs_on: list[Literal["argo", "hpc"]] = ["argo", "hpc"]


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
