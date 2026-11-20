"""Pipeline lego -> Argo Workflow (a dict: yaml.safe_dump it for `argo submit`, or post it to the API).

Values are resolved when the workflow is written: each step becomes one container template with
its argument list filled in (ToolLego.argv). File and directory ports become Argo artifacts:
- a pipeline input is an s3://<bucket>/<key> URL; Argo reads the key from its default artifact
  repository (the bucket is not passed on)
- a link between steps is `from: {{tasks.<step>.outputs.artifacts.<output>}}`
- a tool's `files` are raw artifacts
- an output is stored as files (archive: none), unless its data lego's storage says archive: tgz, in
  which case Argo packs it into one .tgz (its default). Files get an explicit key under
  {{workflow.name}}/{{pod.name}}/<output>/: a directory's ends in "/", because Argo's artifact GC deletes
  the files under a key only when it does (workflow/artifacts/s3/s3.go, v4.1.6), and a file's keeps its
  file name (its extension)
- {run} in args and env becomes {{workflow.name}}, which Argo fills when the pod starts
- an output that can be written in several formats (Port.formats) is written in the one its step chose
  (Step.formats, else the first): {<output>_format} is that data lego's id, and its storage decides the
  archive. The workflow's hydrowf/formats annotation records each pipeline output's data lego as
  id@version, the version being the latest one at render time

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

from hydrowf_core.models.lego import (DataLego, DataSource, Lego, Link, Pipeline, PipelineInput, Port, Scalar,
                                      StacStorage, Step, ToolLego, Value, chosen_format, fill, format_name,
                                      value_problem)

FILE_TYPES = ("file", "directory")
RUN_NAME = "{{workflow.name}}"          # {run}: Argo puts the workflow's name here when the pod starts
# Caller text may not hold what a later stage reads as its own syntax: Argo's {{...}} (expanded, and
# expanded again, in args, env, paths and keys; there is no escape) and Kubernetes' $(VAR). Single
# characters, not sequences: caller text is joined to registry text, which could complete them.
TEMPLATE_SYNTAX = re.compile(r"[{}$\x00-\x1f]")        # and control characters (newlines in args)
MAX_KEEP_DAYS = 3650                    # ttlStrategy's seconds are an int32 in Argo: under 24855 days
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


def resources(tool: ToolLego) -> dict:
    """The step container's Kubernetes resources, from the tool's cpu and ram_mb.

    requests: what the scheduler reserves on a node for the step (where it can run).
    limits: the most the step may use once running. Past a memory limit the kernel kills the
    container (OOMKilled, the step fails); past a CPU limit it is only slowed down (throttled).
    Without limits a step can take a whole node from everyone else's steps.
    """
    requests = {"cpu": fill("{cpu}", {"cpu": tool.cpu}), "memory": f"{tool.ram_mb}Mi"}
    # Memory is capped at the request: a step that outgrows its ram_mb fails alone, instead of
    # pushing the node out of memory and taking other users' steps with it. CPU is not capped:
    # a step may use idle cores, and when cores are short Kubernetes shares them by request, so a
    # busy step slows the others down to their requested cores at worst, never below.
    return {"requests": requests, "limits": {"memory": requests["memory"]}}


def latest_data(legos: Iterable[Lego]) -> dict[str, DataLego]:
    """Data lego id -> its latest version (by SemVer): the one render uses for archive and records."""
    latest: dict[str, DataLego] = {}
    for lego in legos:
        if isinstance(lego, DataLego) and (lego.id not in latest or
                                           semver(lego.version) > semver(latest[lego.id].version)):
            latest[lego.id] = lego
    return latest


def output_archives(legos: Iterable[Lego]) -> dict[str, str]:
    """Data lego id -> how its outputs are stored ("none" or "tgz"), from the latest version of each
    data lego, when that version has STAC storage. An output whose data isn't here is stored as files."""
    return {id: lego.storage.archive for id, lego in latest_data(legos).items()
            if isinstance(lego.storage, StacStorage)}


def output_formats(pipeline: Pipeline, legos: Iterable[Lego]) -> dict[str, str]:
    """Pipeline output name -> the data lego it is written as, id@version: the format its step chose, at
    that data lego's latest version. Outputs whose tool or data lego isn't in `legos` are left out."""
    legos = list(legos)
    tools = {(x.id, x.version): x for x in legos if isinstance(x, ToolLego)}
    latest = latest_data(legos)
    steps = {s.name: s for s in pipeline.steps}
    found = {}
    for out, link in pipeline.outputs.items():
        step = steps[link.step]
        tool = tools.get(step.tool.key())
        port = next((p for p in tool.outputs if p.name == link.output), None) if tool else None
        data = chosen_format(step, port) if port else None
        if data in latest:
            found[out] = f"{data}@{latest[data].version}"
    return found


def semver(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in version.split("."))      # SemVer: at most 32 characters


OUTPUT_PREFIX = "{{workflow.name}}/{{pod.name}}"     # the artifact repository's default key format


def output_artifact(port: Port, path: str, archives: Mapping[str, str], data: str | None = None) -> dict:
    """The Argo output artifact of `port`, stored as its data lego says: `data`, the format the step
    writes (default: the port's first)."""
    name = k8s_name(port.name)
    artifact = {"name": name, "path": path}
    data = data or next(iter(port.choices), "")
    if archives.get(data, "none") == "tgz":
        return artifact                         # Argo's default: one <name>.tgz under OUTPUT_PREFIX
    artifact["archive"] = {"none": {}}          # each file its own object
    if port.type == "directory":                # the trailing "/" makes artifact GC delete the files
        artifact["s3"] = {"key": f"{OUTPUT_PREFIX}/{name}/"}
    else:                                       # its own folder: two outputs can share a file name
        artifact["s3"] = {"key": f"{OUTPUT_PREFIX}/{name}/{path.rsplit('/', 1)[-1]}"}
    return artifact


def container_template(step: Step, tool: ToolLego, scalars: dict[str, Scalar | None], staged: Iterable[str],
                       entrypoint: Entrypoint | None, archives: Mapping[str, str] | None = None) -> dict:
    """One step's Argo template: stage inputs and files under the workdir, run, collect outputs."""
    formats = {p.name: chosen_format(step, p) for p in tool.outputs}
    runtime = {**scalars, "workdir": tool.workdir, "cpu": tool.cpu, "run": RUN_NAME,
               **{format_name(p.name): formats[p.name] for p in tool.outputs if p.formats}}

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
    container["resources"] = resources(tool)
    container["volumeMounts"] = [{"name": "work", "mountPath": tool.workdir}]

    return {
        "name": f"step-{step.name}",
        "inputs": {"artifacts": in_artifacts},
        "outputs": {"artifacts": [output_artifact(p, at(p.path), archives or {}, formats[p.name])
                                  for p in tool.outputs if p.type in FILE_TYPES]},
        "volumes": [{"name": "work", "emptyDir": {}}],
        "container": container,
    }


def render(pipeline: Pipeline, legos: Iterable[Lego], inputs: Mapping[str, Scalar],
           entrypoint: Entrypoint | None = None, name: str | None = None,
           labels: Mapping[str, str] | None = None, keep_days: int | None = None) -> dict:
    """An Argo Workflow that runs `pipeline` once with `inputs`.

    `legos` must contain the steps' tools (registry.check has passed on them). `entrypoint(image)`
    gives the ENTRYPOINT of images whose tool leaves `command` empty. `name` names the workflow (the
    caller's id for the run, e.g. a pygeoapi job id); without it Argo generates `<pipeline id>-xxxxx`.
    Argo copies the name into a pod label, so keep it a DNS name of 63 characters or less.
    `labels` are the caller's own workflow labels (e.g. who submitted the run); they can't replace
    hydrowf/pipeline or hydrowf/version.

    Deleting the workflow deletes its output artifacts and archived logs too (artifactGC). With
    `keep_days`, Argo deletes the workflow, and so its outputs, that many days after it finishes.
    """
    if keep_days is not None and (isinstance(keep_days, bool) or not isinstance(keep_days, int)
                                  or not 1 <= keep_days <= MAX_KEEP_DAYS):
        raise ValueError(f"keep_days={keep_days!r}: a whole number of days, 1 to {MAX_KEEP_DAYS}")
    extra = check_labels(labels or {})
    if reserved := sorted(set(extra) & set(RESERVED_LABELS)):
        raise ValueError(f"{', '.join(reserved)}: render() sets these labels itself")
    legos = list(legos)
    tools = {(x.id, x.version): x for x in legos if isinstance(x, ToolLego)}
    archives = output_archives(legos)
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
        templates.append(container_template(step, tool, scalars, artifacts, entrypoint, archives))
        task: dict = {"name": step_name, "template": f"step-{step_name}"}
        deps = sorted({src.step for src in step.inputs.values() if isinstance(src, Link)})
        if deps:
            task["dependencies"] = deps
        if artifacts:
            task["arguments"] = {"artifacts": [{"name": k8s_name(n), **src} for n, src in artifacts.items()]}
        tasks.append(task)

    outputs = {out: f"{link.step}/{k8s_name(link.output)}" for out, link in pipeline.outputs.items()}
    formats = output_formats(pipeline, legos)
    return {
        "apiVersion": "argoproj.io/v1alpha1",
        "kind": "Workflow",
        "metadata": {
            **({"name": name} if name else {"generateName": f"{pipeline.id}-"}),
            "labels": {**extra, "hydrowf/pipeline": pipeline.id, "hydrowf/version": pipeline.version},
            "annotations": {
                "hydrowf/outputs": json.dumps(outputs),       # pipeline output -> task/artifact
                "hydrowf/formats": json.dumps(formats),       # pipeline output -> data lego id@version
            },
        },
        "spec": {
            "entrypoint": "main",
            "artifactGC": {"strategy": "OnWorkflowDeletion"},
            **({"ttlStrategy": {"secondsAfterCompletion": keep_days * 86400}} if keep_days is not None else {}),
            "templates": [{"name": "main", "dag": {"tasks": tasks}}] + templates,
        },
    }
