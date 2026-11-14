"""Cross-lego checks: run on the whole registry (or a registry plus a lego about to be saved).

The models validate each lego on its own. What needs other legos is here:
- a tool's file/directory/dataset port names a data lego that exists
- a pipeline step's tool exists (at the pinned version) and is a tool
- every step input is a port of that tool; every required port is given
- each source fits its port: same kind (scalar type, file, directory, dataset) and same data lego id

Returns a list of problems instead of raising on the first one, so a form can show them all.
"""
from __future__ import annotations

from collections.abc import Iterable

from hydrowf_core.models.lego import (DataLego, DataSource, Lego, Link, Pipeline, PipelineInput, Port, ToolLego,
                                      Value)

# Python types a constant may have for each scalar port type (bool is an int in Python: keep it apart).
VALUE_TYPES = {"string": (str,), "enum": (str,), "int": (int,), "float": (int, float), "boolean": (bool,)}


def check(legos: Iterable[Lego]) -> list[str]:
    legos = list(legos)
    by_key = {(lego.id, lego.version): lego for lego in legos}
    problems = [f"{k[0]}@{k[1]} is defined more than once"
                for k in by_key if sum((x.id, x.version) == k for x in legos) > 1]
    data_ids = {lego.id for lego in legos if isinstance(lego, DataLego)}
    for lego in legos:
        if isinstance(lego, ToolLego):
            for port in lego.inputs + lego.outputs:
                if port.data and port.data not in data_ids:
                    problems.append(f"tool {lego.id}@{lego.version}: port {port.name}: "
                                    f"unknown data lego {port.data!r}")
        elif isinstance(lego, Pipeline):
            problems += check_pipeline(lego, by_key)
    return problems


def check_pipeline(p: Pipeline, by_key: dict[tuple[str, str], Lego]) -> list[str]:
    where = f"pipeline {p.id}@{p.version}"
    problems: list[str] = []
    tools: dict[str, ToolLego] = {}
    for s in p.steps:
        tool = by_key.get(s.tool.key())
        if isinstance(tool, ToolLego):
            tools[s.name] = tool
        else:
            problems.append(f"{where}: step {s.name}: {s.tool.id}@{s.tool.version} is not a registered tool")

    pipeline_inputs = {port.name: port for port in p.inputs}
    for s in p.steps:
        tool = tools.get(s.name)
        if tool is None:
            continue
        ports = {port.name: port for port in tool.inputs}
        for name in sorted(s.inputs.keys() - ports.keys()):
            problems.append(f"{where}: step {s.name}: {tool.id} has no input {name!r}")
        for name, port in ports.items():
            src = s.inputs.get(name)
            if src is None:
                if not port.optional:
                    problems.append(f"{where}: step {s.name}: required input {name!r} is not connected")
                continue
            problem = source_problem(port, src, pipeline_inputs, tools, by_key)
            if problem:
                problems.append(f"{where}: step {s.name}.{name}: {problem}")

    for out, link in p.outputs.items():
        tool = tools.get(link.step)
        if tool and link.output not in {port.name for port in tool.outputs}:
            problems.append(f"{where}: output {out}: {tool.id} has no output {link.output!r}")
    return problems


def source_problem(port: Port, src: Value | PipelineInput | Link | DataSource,
                   pipeline_inputs: dict[str, Port], tools: dict[str, ToolLego],
                   by_key: dict[tuple[str, str], Lego]) -> str | None:
    """Why `src` can't feed `port`, or None if it can."""
    if isinstance(src, Value):
        allowed = VALUE_TYPES.get(port.type)
        if allowed is None:
            return f"a {port.type} port can't take a constant"
        if isinstance(src.value, bool) and bool not in allowed:
            return f"{src.value!r} is a boolean, the port is {port.type}"
        if not isinstance(src.value, allowed):
            return f"{src.value!r} is not a {port.type}"
        if port.type == "enum" and src.value not in port.symbols:
            return f"{src.value!r} is not one of {port.symbols}"
        return None
    if isinstance(src, DataSource):
        data = by_key.get(src.data.key())
        if not isinstance(data, DataLego):
            return f"{src.data.id}@{src.data.version} is not a registered data lego"
        if port.type != "dataset" or port.data != data.id:
            return f"data lego {data.id} fits a dataset port of {data.id}, not a {port.type} port of {port.data}"
        return None
    if isinstance(src, PipelineInput):
        upstream = pipeline_inputs[src.input]          # declared: the Pipeline model checked it
        what = f"pipeline input {src.input}"
    else:
        tool = tools.get(src.step)
        if tool is None:
            return None                                 # already reported: unknown tool
        upstream = next((o for o in tool.outputs if o.name == src.output), None)
        if upstream is None:
            return f"{tool.id} has no output {src.output!r}"
        what = f"{src.step}.{src.output}"
    if (upstream.type, upstream.data) != (port.type, port.data):
        return (f"{what} is {upstream.type}{f' of {upstream.data}' if upstream.data else ''}, "
                f"the port wants {port.type}{f' of {port.data}' if port.data else ''}")
    return None
