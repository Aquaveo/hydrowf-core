"""Pipeline lego -> Argo Workflow (a dict: yaml.safe_dump it for `argo submit`, or post it to the API).

Values are resolved when the workflow is written: each step becomes one container template with
its argument list filled in (ToolLego.argv). File and directory ports become Argo artifacts:
- a pipeline input is an s3://<bucket>/<key> URL; Argo reads the key from its default artifact
  repository (the bucket is not passed on)
- a link between steps is `from: {{tasks.<step>.outputs.artifacts.<output>}}`
- a tool's `files` are raw artifacts
- {run} in args and env becomes {{workflow.name}}, which Argo fills when the pod starts

What running on Argo v4.1.6 taught us (hydrowf-platform docs/argo-findings.md):
- the emissary executor needs the container `command`; with `args` only it asks the image registry
  for the ENTRYPOINT, which fails for images the registry won't describe (local, private). Pass
  `entrypoint` for tools that leave `command` empty, or the image must be in a public registry.
- an output artifact is one exact path.
- the workdir is an emptyDir volume; inputs are staged inside it.
"""
from __future__ import annotations

import json
import re
from collections.abc import Callable, Iterable, Mapping

from hydrowf_core.models.lego import (DataSource, Lego, Link, Pipeline, PipelineInput, Scalar, Step, ToolLego, Value,
                                      fill, value_problem)

FILE_TYPES = ("file", "directory")
RUN_NAME = "{{workflow.name}}"          # {run}: Argo puts the workflow's name here when the pod starts
# Caller text may not hold what a later stage reads as its own syntax: Argo's {{...}} (expanded, and
# expanded again, in args, env, paths and keys; there is no escape) and Kubernetes' $(VAR). Single
# characters, not sequences: caller text is joined to registry text, which could complete them.
TEMPLATE_SYNTAX = re.compile(r"[{}$\x00-\x1f]")        # and control characters (newlines in args)
MAX_TEXT = 1024                         # before any registry pattern runs on it
Entrypoint = Callable[[str], "list[str] | None"]


class UnsupportedForArgo(ValueError):
    """The pipeline uses something this adapter can't run yet."""


LABEL_VALUE = re.compile(r"(?:[A-Za-z0-9](?:[-A-Za-z0-9_.]{0,61}[A-Za-z0-9])?)?")   # Kubernetes' rule
LABEL_NAME = r"[A-Za-z0-9](?:[-A-Za-z0-9_.]{0,61}[A-Za-z0-9])?"
DNS_LABEL = r"[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?"
LABEL_KEY = re.compile(rf"(?:(?=[^/]{{1,253}}/){DNS_LABEL}(?:\.{DNS_LABEL})*/)?{LABEL_NAME}")   # [prefix/]name
RESERVED_LABELS = ("hydrowf/pipeline", "hydrowf/version")       # render() sets these itself


def check_labels(labels: Mapping[str, str]) -> dict[str, str]:
    """`labels` if every key and value is a valid Kubernetes label; ValueError otherwise. Callers build
    label selectors from them, so a value like "a,b" or "x!=y" must never get through."""
    for key, value in labels.items():
        if not (isinstance(key, str) and isinstance(value, str)
                and LABEL_KEY.fullmatch(key) and LABEL_VALUE.fullmatch(value)):
            raise ValueError(f"{key}={value!r} is not a valid Kubernetes label")
    return dict(labels)


class BadInputs(ValueError):
    """The run's input values don't fit the pipeline's inputs."""


def k8s_name(name: str) -> str:
    return name.replace("_", "-")


def s3_key(url: object) -> str:
    if not isinstance(url, str) or not url.startswith("s3://") or url.count("/") < 3:
        raise BadInputs(f"{url!r}: a file or directory input is an s3://<bucket>/<key> URL")
    return url.split("/", 3)[3]


def pipeline_values(pipeline: Pipeline, inputs: Mapping[str, Scalar]) -> dict[str, Scalar | None]:
    """The run's value for every pipeline input (given, or the port's default); checks each one."""
    unknown = sorted(inputs.keys() - {p.name for p in pipeline.inputs})
    if unknown:
        raise BadInputs(f"{pipeline.id} has no inputs {unknown}")
    values: dict[str, Scalar | None] = {}
    for port in pipeline.inputs:
        value = inputs.get(port.name, port.default)
        if port.name in inputs and isinstance(value, str):     # the caller's text; defaults are the registry's
            if len(value) > MAX_TEXT:
                raise BadInputs(f"input {port.name}: longer than {MAX_TEXT} characters")
            if TEMPLATE_SYNTAX.search(value):
                raise BadInputs(f"input {port.name}: {{, }}, $ and control characters are not allowed "
                                "(Argo and Kubernetes would read them as templates)")
        if value is None:
            if not port.optional:
                raise BadInputs(f"input {port.name} is required")
        elif port.type in FILE_TYPES:
            s3_key(value)
        elif port.type == "dataset":
            raise UnsupportedForArgo(f"input {port.name}: dataset inputs need a data driver")
        elif problem := value_problem(port.type, port.symbols, value, port.pattern):
            raise BadInputs(f"input {port.name}: {problem}")
        values[port.name] = value
    return values


def task_inputs(step: Step, tool: ToolLego,
                values: dict[str, Scalar | None]) -> tuple[dict[str, Scalar | None], dict[str, dict]]:
    """Where each of the tool's inputs comes from, for one step.

    Returns (scalars, artifacts):
    - scalars: input name -> value, for every non-file input (unset optional ones as None)
    - artifacts: input name -> Argo artifact source, for file/directory inputs that have one:
      {"s3": {"key": ...}} from a pipeline input, {"from": "{{tasks...}}"} from a link
    """
    scalars: dict[str, Scalar | None] = {}
    artifacts: dict[str, dict] = {}
    for port in tool.inputs:                      # the ports, so unconnected ones get their default
        src = step.inputs.get(port.name)
        if isinstance(src, DataSource):
            raise UnsupportedForArgo(f"step {step.name}.{port.name}: dataset inputs need a data driver")
        if isinstance(src, Link):
            artifacts[port.name] = {"from": f"{{{{tasks.{src.step}.outputs.artifacts.{k8s_name(src.output)}}}}}"}
            continue
        if isinstance(src, Value):
            value = src.value
        elif isinstance(src, PipelineInput):
            value = values[src.input]
            if value is not None and port.pattern is not None and (
                    problem := value_problem(port.type, port.symbols, value, port.pattern)):
                raise BadInputs(f"input {src.input} (step {step.name}.{port.name}): {problem}")
        else:
            value = port.default
        if port.type not in FILE_TYPES:
            scalars[port.name] = value            # None too: argv needs every name
        elif value is not None:                   # an unset optional file is simply not staged
            artifacts[port.name] = {"s3": {"key": s3_key(value)}}
    return scalars, artifacts


def container_template(step: Step, tool: ToolLego, scalars: dict[str, Scalar | None], staged: Iterable[str],
                       entrypoint: Entrypoint | None) -> dict:
    """One step's Argo template: stage inputs and files under the workdir, run, collect outputs."""
    runtime = {**scalars, "workdir": tool.workdir, "cpu": tool.cpu, "run": RUN_NAME}

    def at(path: str) -> str:
        filled = fill(path, runtime)          # inputs may fill it: check what they made, not the template
        if filled.startswith("/") or any(part in ("", ".", "..") for part in filled.split("/")):
            raise BadInputs(f"step {step.name}: path {path!r} becomes {filled!r}, outside {tool.workdir}")
        return f"{tool.workdir}/{filled}"

    paths = {p.name: at(p.path or f"inputs/{p.name}") for p in tool.inputs if p.name in staged}
    values = {**runtime, **{p.name: None for p in tool.inputs if p.type in FILE_TYPES}, **paths}
    in_artifacts = [{"name": k8s_name(name), "path": path} for name, path in paths.items()]
    in_artifacts += [{"name": f"file-{n}", "path": at(path), "raw": {"data": content}}
                     for n, (path, content) in enumerate(tool.files.items())]

    command = tool.command if tool.command is not None else (entrypoint(tool.image) if entrypoint else None)
    container: dict = {"image": tool.image}
    if command:
        container["command"] = list(command)
    container["args"] = tool.argv(values)
    container["workingDir"] = tool.workdir
    if tool.env:
        container["env"] = [{"name": k, "value": fill(v, values)} for k, v in tool.env.items()]
    container["resources"] = {"requests": {"cpu": fill("{cpu}", values), "memory": f"{tool.ram_mb}Mi"}}
    container["volumeMounts"] = [{"name": "work", "mountPath": tool.workdir}]

    return {
        "name": f"step-{step.name}",
        "inputs": {"artifacts": in_artifacts},
        "outputs": {"artifacts": [{"name": k8s_name(p.name), "path": at(p.path)}
                                  for p in tool.outputs if p.type in FILE_TYPES]},
        "volumes": [{"name": "work", "emptyDir": {}}],
        "container": container,
    }


def render(pipeline: Pipeline, legos: Iterable[Lego], inputs: Mapping[str, Scalar],
           entrypoint: Entrypoint | None = None, name: str | None = None,
           labels: Mapping[str, str] | None = None) -> dict:
    """An Argo Workflow that runs `pipeline` once with `inputs`.

    `legos` must contain the steps' tools (registry.check has passed on them). `entrypoint(image)`
    gives the ENTRYPOINT of images whose tool leaves `command` empty. `name` names the workflow (the
    caller's id for the run, e.g. a pygeoapi job id); without it Argo generates `<pipeline id>-xxxxx`.
    Argo copies the name into a pod label, so keep it a DNS name of 63 characters or less.
    `labels` are the caller's own workflow labels (e.g. who submitted the run); they can't replace
    hydrowf/pipeline or hydrowf/version.
    """
    extra = check_labels(labels or {})
    if reserved := sorted(set(extra) & set(RESERVED_LABELS)):
        raise ValueError(f"{', '.join(reserved)}: render() sets these labels itself")
    tools = {(x.id, x.version): x for x in legos if isinstance(x, ToolLego)}
    values = pipeline_values(pipeline, inputs)
    steps = {s.name: s for s in pipeline.steps}
    templates, tasks = [], []
    for step_name in pipeline.order():
        step = steps[step_name]
        tool = tools.get(step.tool.key())
        if tool is None:
            raise UnsupportedForArgo(f"step {step_name}: {step.tool.id}@{step.tool.version} is not a registered tool")
        if "argo" not in tool.runs_on:
            raise UnsupportedForArgo(f"step {step_name}: {tool.id} does not run on argo")
        scalars, artifacts = task_inputs(step, tool, values)
        templates.append(container_template(step, tool, scalars, artifacts, entrypoint))
        task: dict = {"name": step_name, "template": f"step-{step_name}"}
        deps = sorted({src.step for src in step.inputs.values() if isinstance(src, Link)})
        if deps:
            task["dependencies"] = deps
        if artifacts:
            task["arguments"] = {"artifacts": [{"name": k8s_name(n), **src} for n, src in artifacts.items()]}
        tasks.append(task)

    outputs = {out: f"{link.step}/{k8s_name(link.output)}" for out, link in pipeline.outputs.items()}
    return {
        "apiVersion": "argoproj.io/v1alpha1",
        "kind": "Workflow",
        "metadata": {
            **({"name": name} if name else {"generateName": f"{pipeline.id}-"}),
            "labels": {**extra, "hydrowf/pipeline": pipeline.id, "hydrowf/version": pipeline.version},
            "annotations": {"hydrowf/outputs": json.dumps(outputs)},      # pipeline output -> task/artifact
        },
        "spec": {"entrypoint": "main", "templates": [{"name": "main", "dag": {"tasks": tasks}}] + templates},
    }
