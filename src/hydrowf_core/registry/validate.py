"""Cross-lego checks: run on the whole registry (or a registry plus a lego about to be saved).

The models validate each lego on its own. What needs other legos is here:
- a tool's file/directory/dataset port names a data lego that exists
- a pipeline step's tool exists (at the pinned version) and is a tool
- every step input is a port of that tool; every required port is given
- each source fits its port: same kind (scalar type, file, directory, dataset) and same data lego id
  (an input with a default may stay unconnected); a link carries the format its step chose, and only a
  file or directory output can be linked: into a port of its own kind, or into a later step's dataset
  port of its data lego, read in place unless that lego packs outputs into one .tgz (design D.7)
- a step's format choices name outputs that list formats, and one of their formats
- a data lego feeds a port its driver can serve: external data a dataset port of a tool that declares
  `network: true` (the tool reads the URL in place); managed STAC data a file or directory port (staged
  from its `location`: a key ending in "/" is a directory) or a dataset port (read in place with keys the
  runtime hands the step, render's `reader`); Iceberg data a dataset port (no runtime
  adapter reads it yet: render refuses it, check doesn't, since a pipeline's wiring doesn't depend on
  the runtime)

Returns a list of problems instead of raising on the first one, so a form can show them all.
"""
from __future__ import annotations

from collections.abc import Iterable

from hydrowf_core.models.lego import (DataLego, DataSource, ExternalStorage, Lego, Link, Pipeline, PipelineInput,
                                      Port, StacStorage, Step, ToolLego, Value, chosen_format, semver,
                                      value_problem)


def check(legos: Iterable[Lego]) -> list[str]:
    legos = list(legos)
    by_key = {(lego.id, lego.version): lego for lego in legos}
    problems = [f"{k[0]}@{k[1]} is defined more than once"
                for k in by_key if sum((x.id, x.version) == k for x in legos) > 1]
    data_ids = {lego.id for lego in legos if isinstance(lego, DataLego)}
    for lego in legos:
        if isinstance(lego, ToolLego):
            for port in lego.inputs + lego.outputs:
                for data in port.choices:
                    if data not in data_ids:
                        problems.append(f"tool {lego.id}@{lego.version}: port {port.name}: "
                                        f"unknown data lego {data!r}")
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
    steps = {s.name: s for s in p.steps}
    for s in p.steps:
        tool = tools.get(s.name)
        if tool is None:
            continue
        problems += [f"{where}: step {s.name}: {problem}" for problem in format_problems(s, tool)]
        ports = {port.name: port for port in tool.inputs}
        for name in sorted(s.inputs.keys() - ports.keys()):
            problems.append(f"{where}: step {s.name}: {tool.id} has no input {name!r}")
        for name, port in ports.items():
            src = s.inputs.get(name)
            if src is None:
                if not port.optional and port.default is None:
                    problems.append(f"{where}: step {s.name}: required input {name!r} is not connected")
                continue
            problem = source_problem(port, src, pipeline_inputs, tools, by_key, steps, tool)
            if problem:
                problems.append(f"{where}: step {s.name}.{name}: {problem}")

    for out, link in p.outputs.items():
        tool = tools.get(link.step)
        if tool and link.output not in {port.name for port in tool.outputs}:
            problems.append(f"{where}: output {out}: {tool.id} has no output {link.output!r}")
    return problems


def data_source_problem(port: Port, src: DataSource, by_key: dict[tuple[str, str], Lego],
                        tool: ToolLego) -> str | None:
    """Why the data lego `src` names can't feed `port` of `tool` (see the module docstring)."""
    if port.type not in ("file", "directory", "dataset"):
        return f"a {port.type} port can't take a data lego"
    data = by_key.get(src.data.key())
    if not isinstance(data, DataLego):
        return f"{src.data.id}@{src.data.version} is not a registered data lego"
    if port.data != data.id:
        return f"data lego {data.id} doesn't fit a port of {port.data}"
    storage = data.storage
    if isinstance(storage, ExternalStorage):
        if port.type != "dataset":
            return f"external data {data.id} is read in place: it feeds a dataset port, not a {port.type} port"
        if not tool.network:
            return f"external data {data.id} is read from the internet: {tool.id} must declare network: true"
        return None
    if isinstance(storage, StacStorage) and storage.location is not None:
        return managed_problem(data.id, storage.location, port.type)
    if isinstance(storage, StacStorage):
        return f"{data.id} is output data: a run's output is passed as a link or an s3:// input, not by id"
    if port.type != "dataset":                           # Iceberg: tables, read in place
        return f"{storage.driver} data {data.id} is read in place: it feeds a dataset port, not a {port.type} port"
    return None                                          # whether a runtime can read it is the adapter's call


def managed_problem(data_id: str, location: str, port_type: str) -> str | None:
    """Why managed data at `location` can't feed a `port_type` port: Argo stages it, with its own
    credentials, as the file or directory the location names; a dataset port reads it in place, with
    read keys the runtime hands the step (render's `reader`)."""
    if port_type == "dataset":
        return None
    kind = "directory" if location.endswith("/") else "file"
    if kind != port_type:
        return f"managed data {data_id} is a {kind} ({location}), the port wants a {port_type}"
    return None


def linked_read_problem(what: str, data: str, by_key: dict[tuple[str, str], Lego]) -> str | None:
    """Why a later step's dataset port can't read the step output `what` (of data lego `data`) in place: the
    data lego's newest version, which render stores the output by, keeps it as one .tgz."""
    versions = [x for x in by_key.values() if isinstance(x, DataLego) and x.id == data]
    newest = max(versions, key=lambda x: semver(x.version), default=None)
    if newest is not None and isinstance(newest.storage, StacStorage) and newest.storage.archive == "tgz":
        return (f"{what} is stored as one .tgz ({data}: archive tgz): a dataset port reads files in place, "
                "so it can't read it")
    return None


def format_problems(step: Step, tool: ToolLego) -> list[str]:
    """What is wrong with `step`'s format choices for `tool`."""
    outputs = {port.name: port for port in tool.outputs}
    problems = []
    for output, data in step.formats.items():
        port = outputs.get(output)
        if port is None:
            problems.append(f"formats: {tool.id} has no output {output!r}")
        elif not port.formats:
            problems.append(f"formats: output {output!r} of {tool.id} has one format, {port.data!r}")
        elif data not in port.formats:
            problems.append(f"formats: output {output!r} of {tool.id} writes {', '.join(port.formats)}, "
                            f"not {data!r}")
    return problems


def source_problem(port: Port, src: Value | PipelineInput | Link | DataSource,
                   pipeline_inputs: dict[str, Port], tools: dict[str, ToolLego],
                   by_key: dict[tuple[str, str], Lego], steps: dict[str, Step], tool: ToolLego) -> str | None:
    """Why `src` can't feed `port` of `tool`, or None if it can."""
    if isinstance(src, Value):
        return value_problem(port.type, port.symbols, src.value, port.pattern)
    if isinstance(src, DataSource):
        return data_source_problem(port, src, by_key, tool)
    if isinstance(src, PipelineInput):
        upstream = pipeline_inputs[src.input]          # declared: the Pipeline model checked it
        what = f"pipeline input {src.input}"
        data = upstream.data
        if upstream.type == port.type and upstream.default is not None and port.pattern is not None and (
                problem := value_problem(port.type, port.symbols, upstream.default, port.pattern)):
            return f"{what}'s default {problem}"       # a registry mistake: at load, not on execute
    else:
        tool = tools.get(src.step)
        if tool is None:
            return None                                 # already reported: unknown tool
        upstream = next((o for o in tool.outputs if o.name == src.output), None)
        if upstream is None:
            return f"{tool.id} has no output {src.output!r}"
        what = f"{src.step}.{src.output}"
        if upstream.type not in ("file", "directory"):
            return f"{what} is a {upstream.type} output: only file and directory outputs can be linked"
        data = chosen_format(steps[src.step], upstream)  # the format that step writes
        if port.type == "dataset" and data == port.data:
            return linked_read_problem(what, data, by_key)   # read in place from a key render gives it (D.7)
    if (upstream.type, data) != (port.type, port.data):
        return (f"{what} is {upstream.type}{f' of {data}' if data else ''}, "
                f"the port wants {port.type}{f' of {port.data}' if port.data else ''}")
    return None
