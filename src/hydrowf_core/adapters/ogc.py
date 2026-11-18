"""Pipeline lego -> OGC API - Processes process description (the dict pygeoapi serves at /processes/{id}).

The shape is pygeoapi's PROCESS_METADATA (pygeoapi/process/hello_world.py). pygeoapi adds the rest
itself: it sets `id` from its config key and `outputTransmission` when missing.

Only the pipeline's own inputs are exposed: a step input the pipeline fixes or leaves at the tool's
default is not part of the API. File and directory ports are s3:// URIs, which is what the Argo
adapter stages (argo.s3_key). Outputs are the s3:// links ArgoRuntime.outputs returns.
"""
from __future__ import annotations

from collections.abc import Iterable

from hydrowf_core.models.lego import DataLego, Lego, Pipeline, PipelineInput, Port, ToolLego, anchored

S3_URI = r"^s3://[^/]+/.+"
JSON_TYPES = {"string": "string", "int": "integer", "float": "number", "boolean": "boolean"}


class UnsupportedForOgc(ValueError):
    """The pipeline has something the process description can't express yet."""


def port_schema(port: Port) -> dict:
    """The JSON Schema of one port's value, e.g. {"type": "integer", "default": 6}."""
    match port:
        case Port(type="enum", symbols=symbols):
            schema = {"type": "string", "enum": list(symbols)}
        case Port(type="file" | "directory"):
            schema = {"type": "string", "format": "uri", "pattern": S3_URI}
        case Port(type=t) if t in JSON_TYPES:
            schema = {"type": JSON_TYPES[t]}
            if port.pattern is not None:      # JSON Schema patterns aren't anchored; ours match whole values
                schema["pattern"] = anchored(port.pattern)
        case _:
            raise UnsupportedForOgc(f"port {port.name}: {port.type} ports need a data driver")
    if port.default is not None:          # False and 0 are real defaults
        schema["default"] = port.default
    return schema


def describe(port: Port, data_titles: dict[str, str]) -> str:
    """The port's description, followed by the title of the data lego it carries."""
    parts = [port.description] if port.description else []
    if port.data in data_titles:
        parts.append(f"({data_titles[port.data]})")
    return " ".join(parts)


def patterns(port: Port, pipeline: Pipeline, tools: dict) -> list[str]:
    """Every pattern a value of pipeline input `port` must match: its own, then those of the tool ports
    it feeds (render enforces both)."""
    found = [port.pattern] if port.pattern else []
    for step in pipeline.steps:
        tool = tools.get(step.tool.key())
        for name, src in step.inputs.items():
            if isinstance(src, PipelineInput) and src.input == port.name and tool is not None:
                fed = next((p for p in tool.inputs if p.name == name), None)
                if fed is not None and fed.pattern and fed.pattern not in found:
                    found.append(fed.pattern)
    return found


def process_description(pipeline: Pipeline, legos: Iterable[Lego]) -> dict:
    """The OGC process description of `pipeline`.

    `legos` must contain the steps' tools and the data legos their ports carry (registry.check has
    passed on them).
    """
    legos = list(legos)
    tools = {(x.id, x.version): x for x in legos if isinstance(x, ToolLego)}
    data_titles = {x.id: x.title for x in legos if isinstance(x, DataLego)}
    steps = {s.name: s for s in pipeline.steps}

    inputs = {}
    for port in pipeline.inputs:
        if port.type == "dataset":
            raise UnsupportedForOgc(f"input {port.name}: dataset inputs need a data driver")
        schema = port_schema(port)
        if port.type == "string":             # file ports keep port_schema's s3:// pattern
            schema.pop("pattern", None)
            match patterns(port, pipeline, tools):
                case []:
                    pass
                case [one]:
                    schema["pattern"] = anchored(one)
                case several:                 # a value fed to two tools must fit both
                    schema["allOf"] = [{"pattern": anchored(p)} for p in several]
        inputs[port.name] = {
            "title": port.name,
            "description": describe(port, data_titles),
            "schema": schema,
            "minOccurs": 0 if port.optional or port.default is not None else 1,
            "maxOccurs": 1,
        }

    outputs = {}
    for name, link in pipeline.outputs.items():
        tool = tools.get(steps[link.step].tool.key())
        if tool is None:
            raise UnsupportedForOgc(f"output {name}: step {link.step} has no registered tool")
        port = next(o for o in tool.outputs if o.name == link.output)
        outputs[name] = {"title": name, "description": describe(port, data_titles), "schema": port_schema(port)}

    return {
        "id": pipeline.id,
        "version": pipeline.version,
        "title": pipeline.title,
        "description": pipeline.description,
        "jobControlOptions": ["async-execute"],     # a run takes minutes: never block the request
        "keywords": ["hydrowf", "pipeline"],
        "inputs": inputs,
        "outputs": outputs,
    }
