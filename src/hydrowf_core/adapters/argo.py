"""Pipeline lego -> Argo Workflow (a dict: yaml.safe_dump it for `argo submit`, or post it to the API).

Values are resolved when the workflow is written: each step becomes one container template with
its argument list filled in (ToolLego.argv). File and directory ports become Argo artifacts:
- a pipeline input is an s3://<bucket>/<key> URL; Argo reads the key from its default artifact
  repository (the bucket is not passed on)
- a link between steps is `from: {{tasks.<step>.outputs.artifacts.<output>}}`, except into a dataset port
  (below)
- a registered data lego as a step input (DataSource): external data gives a dataset port of a
  network tool its URL (the tool reads it in place); managed STAC data is staged from its `location` key
  for a file or directory port. Iceberg data has no driver here yet.
- a dataset port reads our bucket in place: a managed data lego's location, or a pipeline dataset input
  (an s3:// key in the bucket, like a file input). The tool gets s3://<bucket>/<key>, and the step gets
  read keys for exactly those keys (`Reader`): an init container presents the pod's projected
  service-account token to the vendor and writes an AWS credentials file the tool's SDK reads. The
  workflow's hydrowf/reads annotation records each step's keys, which is what the vendor scopes to.
  A dataset port may also read an earlier step's output in place (a link): that output's key is then
  <run>/<step>/<output>/ (linked_prefix), which render knows; it needs the workflow's name.
- no workflow is named `managed`: its outputs would land under managed/, where managed data lives
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
from dataclasses import dataclass

from hydrowf_core.models.lego import (MANAGED_PREFIX, PLAIN_KEY, DataLego, DataSource, ExternalStorage, Lego, Link,
                                      Pipeline, PipelineInput, Port, Scalar, StacStorage, Step, ToolLego, Value,
                                      chosen_format, fill, format_name, semver, value_problem)
from hydrowf_core.registry.validate import data_source_problem

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


READS_DIR = "/var/run/hydrowf"                 # in a step that reads in place: token/ and aws/ live here
# an image pinned by digest, from a registry that may have a port; nothing Argo or Kubernetes would expand
PINNED_IMAGE = re.compile(r"([A-Za-z0-9.\-]+(:[0-9]+)?/)?[a-z0-9._/\-]+@sha256:[0-9a-f]{64}")
PLAIN_URL = re.compile(r"https?://[A-Za-z0-9.\-]+(:[0-9]+)?(/[A-Za-z0-9._~/\-]*)?")   # no {{ }}, $( ), spaces


@dataclass(frozen=True)
class Reader:
    """How a step that reads our bucket in place gets read keys (hydrowf-core docs/design Part D.4).

    An init container (`image`: anything with curl, pinned by digest) POSTs the pod's projected
    service-account token (audience `audience`) to `vendor_url`, and writes the answer, an AWS credentials
    file, where the tool's SDK finds it (AWS_SHARED_CREDENTIALS_FILE). The tool reaches S3 at `s3_endpoint`
    (AWS_ENDPOINT_URL; on AWS the regional endpoint) in `region`. `bucket` is the artifact repository's.
    """
    image: str
    vendor_url: str
    bucket: str
    s3_endpoint: str
    region: str = "us-east-1"
    audience: str = "hydrowf-vendor"
    token_seconds: int = 600

    def __post_init__(self):
        if not PINNED_IMAGE.fullmatch(self.image):
            raise ValueError(f"reader: image {self.image!r} must be pinned by digest (name@sha256:...)")
        for name in ("vendor_url", "s3_endpoint"):
            if not PLAIN_URL.fullmatch(getattr(self, name)):
                raise ValueError(f"reader: {name} {getattr(self, name)!r} is not a plain http(s) URL")
        if not re.fullmatch(r"[a-z0-9][a-z0-9.\-]{1,61}[a-z0-9]", self.bucket):
            raise ValueError(f"reader: bucket {self.bucket!r} is not an S3 bucket name")
        if not re.fullmatch(r"[a-z0-9-]+", self.region) or not re.fullmatch(r"[A-Za-z0-9.\-]+", self.audience):
            raise ValueError("reader: region and audience are plain names")
        if not 600 <= self.token_seconds <= 86400:            # Kubernetes refuses projected tokens under 10 min
            raise ValueError("reader: token_seconds is 600 to 86400")


def reader_parts(reader: Reader) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    """(volumes, init containers, the tool's env, the tool's volume mounts) for a step that reads in place.
    The token goes in the request body (curl --data-binary @file): no shell, nothing Kubernetes expands."""
    volumes = [
        {"name": "hydrowf-token", "projected": {"sources": [{"serviceAccountToken": {
            "audience": reader.audience, "expirationSeconds": reader.token_seconds, "path": "token"}}]}},
        {"name": "hydrowf-aws", "emptyDir": {"medium": "Memory"}},       # the keys never touch a disk
    ]
    init = [{
        "name": "hydrowf-keys",
        "image": reader.image,
        "command": ["curl", "--fail", "--silent", "--show-error", "--retry", "3", "--retry-connrefused",
                    "--max-time", "30", "--data-binary", f"@{READS_DIR}/token/token",
                    "--output", f"{READS_DIR}/aws/credentials", reader.vendor_url],
        "resources": {"requests": {"cpu": "10m", "memory": "16Mi"}, "limits": {"memory": "64Mi"}},
        "volumeMounts": [{"name": "hydrowf-token", "mountPath": f"{READS_DIR}/token", "readOnly": True},
                         {"name": "hydrowf-aws", "mountPath": f"{READS_DIR}/aws"}],
    }]
    credentials = f"{READS_DIR}/aws/credentials"
    host = reader.s3_endpoint.split("://", 1)[1].split("/", 1)[0]
    env = [
        # botocore, s3fs, xarray, DuckDB: the vended file, our endpoint, and nothing that would win over them
        # (keys in the env come before the file, AWS_PROFILE could name another profile, AWS_CONFIG_FILE add a
        # credential_process, AWS_ENDPOINT_URL_S3 come before AWS_ENDPOINT_URL). Empty means unset, and
        # these override the image's own ENV too.
        {"name": "AWS_SHARED_CREDENTIALS_FILE", "value": credentials},
        {"name": "AWS_PROFILE", "value": "default"},
        {"name": "AWS_CONFIG_FILE", "value": f"{READS_DIR}/aws/config"},          # absent: no extra config
        {"name": "AWS_ACCESS_KEY_ID", "value": ""},
        {"name": "AWS_SECRET_ACCESS_KEY", "value": ""},
        {"name": "AWS_SESSION_TOKEN", "value": ""},
        {"name": "AWS_ENDPOINT_URL", "value": reader.s3_endpoint},
        {"name": "AWS_ENDPOINT_URL_S3", "value": reader.s3_endpoint},
        {"name": "AWS_REGION", "value": reader.region},
        {"name": "AWS_DEFAULT_REGION", "value": reader.region},
        # GDAL and rasterio (COGs): the same file, the endpoint without its scheme, path-style addressing
        {"name": "CPL_AWS_CREDENTIALS_FILE", "value": credentials},
        {"name": "AWS_S3_ENDPOINT", "value": host},
        {"name": "AWS_HTTPS", "value": "YES" if reader.s3_endpoint.startswith("https") else "NO"},
        {"name": "AWS_VIRTUAL_HOSTING", "value": "FALSE"},
    ]
    mounts = [{"name": "hydrowf-aws", "mountPath": f"{READS_DIR}/aws", "readOnly": True}]
    return volumes, init, env, mounts


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
        raise BadInputs(f"{url!r}: a file, directory or dataset input is an s3://<bucket>/<key> URL")
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
        elif port.type in FILE_TYPES or port.type == "dataset":       # a dataset: a key read in place
            s3_key(value)
        elif problem := value_problem(port.type, port.symbols, value, port.pattern):
            raise BadInputs(f"input {port.name}: {problem}")
        values[port.name] = value
    return values


def data_source(step: Step, tool: ToolLego, port: Port, src: DataSource,
                data: Mapping[tuple[str, str], DataLego], reader: Reader | None = None
                ) -> tuple[str | None, dict | None, str | None]:
    """What a registered data lego gives `port`: (a value, None, the key read in place or None) or (None,
    an artifact source, None). The same rules as registry.check, checked again here: an external URL is the
    one thing anyone may publish that reaches a container's command line."""
    where = f"step {step.name}.{port.name}"
    lego = data.get(src.data.key())
    if lego is None:
        raise UnsupportedForArgo(f"{where}: {src.data.id}@{src.data.version} is not a registered data lego")
    problem = data_source_problem(port, src, {src.data.key(): lego}, tool)
    if problem:
        raise UnsupportedForArgo(f"{where}: {problem}")
    storage = lego.storage
    if isinstance(storage, ExternalStorage):
        return storage.url, None, None                           # public: read in place, no keys
    if isinstance(storage, StacStorage) and port.type == "dataset":
        return in_place(where, storage.location, reader), None, storage.location
    if isinstance(storage, StacStorage):
        return None, {"s3": {"key": storage.location}}, None     # staged like an s3:// pipeline input
    raise UnsupportedForArgo(f"{where}: {lego.id} is {storage.driver} data: Argo has no driver for it yet")


def in_place(where: str, key: str, reader: Reader | None) -> str:
    """The s3:// URL a dataset port gets for `key` in our bucket, read in place with the step's keys."""
    if reader is None:
        raise UnsupportedForArgo(f"{where}: reading the bucket in place needs read keys for the step "
                                 "(render's reader: the vendor and its init image)")
    return f"s3://{reader.bucket}/{key}"


def caller_read(where: str, url: object, reader: Reader | None) -> str:
    """The key a caller's dataset input reads in place: in our bucket, a plain key with no empty, `.` or `..`
    part. It becomes what the step's read keys cover (hydrowf/reads), so nothing else gets through: not the
    whole bucket, not another bucket's key moved onto ours."""
    if reader is None:
        in_place(where, "", None)                  # the same refusal
    if not isinstance(url, str) or not url.startswith(f"s3://{reader.bucket}/"):
        raise BadInputs(f"{where}: a dataset input is an s3://{reader.bucket}/<key> URL")
    key = url[len(f"s3://{reader.bucket}/"):]
    if not PLAIN_KEY.fullmatch(key) or any(part in ("", ".", "..") for part in key.removesuffix("/").split("/")):
        raise BadInputs(f"{where}: {key!r} is not a plain key (A-Z a-z 0-9 . _ / -, no empty, . or .. part)")
    return key


def task_inputs(step: Step, tool: ToolLego, values: dict[str, Scalar | None],
                data: Mapping[tuple[str, str], DataLego] | None = None, reader: Reader | None = None,
                input_types: Mapping[str, str] | None = None, linked: Mapping[tuple[str, str], str] | None = None
                ) -> tuple[dict[str, Scalar | None], dict[str, dict], list[str]]:
    """Where each of the tool's inputs comes from, for one step.

    Returns (scalars, artifacts, reads):
    - scalars: input name -> value, for every non-file input (unset optional ones as None)
    - artifacts: input name -> Argo artifact source, for file/directory inputs that have one:
      {"s3": {"key": ...}} from a pipeline input or a managed data lego, {"from": "{{tasks...}}"} from a link
    - reads: the keys in our bucket the step reads in place (dataset ports), which its read keys cover

    `input_types` is the pipeline's input name -> port type: a dataset port reads in place only from a
    `dataset` pipeline input (whose value the caller's service checked as one), never from any other.
    `linked`: (step, output) -> the key of an earlier step's output that a dataset port reads in place.
    """
    scalars: dict[str, Scalar | None] = {}
    artifacts: dict[str, dict] = {}
    reads: list[str] = []
    where = f"step {step.name}"
    for port in tool.inputs:                      # the ports, so unconnected ones get their default
        src = step.inputs.get(port.name)
        if isinstance(src, DataSource):
            value, artifact, read = data_source(step, tool, port, src, data or {}, reader)
            if artifact is not None:
                artifacts[port.name] = artifact
            else:
                scalars[port.name] = value
            reads += [read] if read else []
            continue
        if isinstance(src, Link) and port.type == "dataset":     # read in place from the key render gave it
            key = (linked or {}).get((src.step, src.output))
            if key is None:                       # registry.check refuses such a link; never a KeyError here
                raise UnsupportedForArgo(f"{where}.{port.name}: {src.step}.{src.output} has no key to read in place")
            scalars[port.name] = in_place(f"{where}.{port.name}", key, reader)
            reads.append(key)
            continue
        if isinstance(src, Link):
            artifacts[port.name] = {"from": f"{{{{tasks.{src.step}.outputs.artifacts.{k8s_name(src.output)}}}}}"}
            continue
        if isinstance(src, Value):
            if port.type == "dataset":
                raise UnsupportedForArgo(f"{where}.{port.name}: a dataset port takes a data lego or a "
                                         "dataset input, not a constant")
            value = src.value
        elif isinstance(src, PipelineInput):
            value = values[src.input]
            if value is not None and port.pattern is not None and (
                    problem := value_problem(port.type, port.symbols, value, port.pattern)):
                raise BadInputs(f"input {src.input} (step {step.name}.{port.name}): {problem}")
        else:
            value = port.default
        if port.type == "dataset" and isinstance(src, PipelineInput):
            if (input_types or {}).get(src.input) != "dataset":
                raise UnsupportedForArgo(f"{where}.{port.name}: a dataset port reads only from a dataset "
                                         f"input, not from {src.input!r}")
            if value is not None:                 # the caller's own job output (hydrowf-ogc checks whose)
                key = caller_read(f"{where}.{port.name}", value, reader)
                scalars[port.name] = in_place(f"{where}.{port.name}", key, reader)
                reads.append(key)
            else:
                scalars[port.name] = None
        elif port.type not in FILE_TYPES:
            scalars[port.name] = value            # None too: argv needs every name
        elif value is not None:                   # an unset optional file is simply not staged
            artifacts[port.name] = {"s3": {"key": s3_key(value)}}
    return scalars, artifacts, reads


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


OUTPUT_PREFIX = "{{workflow.name}}/{{pod.name}}"     # the artifact repository's default key format


def output_artifact(port: Port, path: str, archives: Mapping[str, str], data: str | None = None,
                    prefix: str | None = None) -> dict:
    """The Argo output artifact of `port`, stored as its data lego says: `data`, the format the step
    writes (default: the port's first). Its key is under Argo's own prefix (the workflow and pod names it
    fills in), or under `prefix` (<run>/<step>, linked_prefix) for an output a later step reads in place."""
    name = k8s_name(port.name)
    artifact = {"name": name, "path": path}
    data = data or next(iter(port.choices), "")
    if archives.get(data, "none") == "tgz":
        if prefix is not None:                  # registry.check refuses it; a dataset port reads files
            raise UnsupportedForArgo(f"output {port.name} is stored as one .tgz: it can't be read in place")
        return artifact                         # Argo's default: one <name>.tgz under OUTPUT_PREFIX
    artifact["archive"] = {"none": {}}          # each file its own object
    prefix = prefix or OUTPUT_PREFIX
    if port.type == "directory":                # the trailing "/" makes artifact GC delete the files
        artifact["s3"] = {"key": f"{prefix}/{name}/"}
    else:                                       # its own folder: two outputs can share a file name
        artifact["s3"] = {"key": f"{prefix}/{name}/{path.rsplit('/', 1)[-1]}"}
    return artifact


def linked_prefix(run: str, step: str) -> str:
    """Where a step's outputs that a later step reads in place are stored: a key render knows, since the
    later step's argument and its read keys (hydrowf/reads) name it before the run starts. Under the run's
    own prefix and <job>/<x>/<output>-shaped, as hydrowf-ogc's checks of a job's keys expect. The output's
    folder is k8s_name(output), as for every output (my_out: my-out)."""
    return f"{run}/{step}"


def linked_keys(template: dict, step: str, outputs: Iterable[str]) -> dict[tuple[str, str], str]:
    """(step, output) -> the key `template` (container_template's) stores each of these outputs under, for
    the later steps that read them in place. Taken from the template, whose paths the step's inputs filled,
    and checked as caller_read checks a caller's key: a file name a caller's input made can't widen a read."""
    names = {k8s_name(o): o for o in outputs}
    found = {}
    for artifact in template["outputs"]["artifacts"]:
        if artifact["name"] in names:
            key = artifact["s3"]["key"]
            if not PLAIN_KEY.fullmatch(key) or any(p in ("", ".", "..") for p in key.removesuffix("/").split("/")):
                raise BadInputs(f"step {step}: output {names[artifact['name']]}'s key {key!r} is not a plain key "
                                "(A-Z a-z 0-9 . _ / -), so a later step can't read it in place")
            found[(step, names[artifact["name"]])] = key
    return found


def in_place_links(pipeline: Pipeline, tools: Mapping[tuple[str, str], ToolLego]) -> set[tuple[str, str]]:
    """(step, output) for each step output that a later step's dataset port reads in place."""
    found = set()
    for step in pipeline.steps:
        tool = tools.get(step.tool.key())
        for port in tool.inputs if tool else []:
            src = step.inputs.get(port.name)
            if port.type == "dataset" and isinstance(src, Link):
                found.add((src.step, src.output))
    return found


def container_template(step: Step, tool: ToolLego, scalars: dict[str, Scalar | None], staged: Iterable[str],
                       entrypoint: Entrypoint | None, archives: Mapping[str, str] | None = None,
                       reader: Reader | None = None, reads: Iterable[str] = (),
                       linked: Mapping[str, str] | None = None) -> dict:
    """One step's Argo template: stage inputs and files under the workdir, run, collect outputs. `linked`:
    output name -> the key prefix of an output a later step reads in place (linked_prefix)."""
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
    env = [{"name": k, "value": fill(v, values)} for k, v in tool.env.items()]
    container["resources"] = resources(tool)
    container["volumeMounts"] = [{"name": "work", "mountPath": tool.workdir}]
    volumes: list[dict] = [{"name": "work", "emptyDir": {}}]
    template: dict = {"name": f"step-{step.name}"}
    if list(reads):
        if reader is None:
            raise UnsupportedForArgo(f"step {step.name}: reading the bucket in place needs render's reader")
        more_volumes, init, aws_env, mounts = reader_parts(reader)
        names = {e["name"] for e in aws_env}
        # in a step that reads with vended keys the tool sets no AWS_* (nor the other names above)
        env = [e for e in env if not e["name"].startswith("AWS_") and e["name"] not in names] + aws_env
        volumes += more_volumes
        container["volumeMounts"] += mounts
        template["initContainers"] = init
    if env:
        container["env"] = env

    return {
        **template,
        "inputs": {"artifacts": in_artifacts},
        "outputs": {"artifacts": [output_artifact(p, at(p.path), archives or {}, formats[p.name],
                                                  (linked or {}).get(p.name))
                                  for p in tool.outputs if p.type in FILE_TYPES]},
        "volumes": volumes,
        "container": container,
    }


def render(pipeline: Pipeline, legos: Iterable[Lego], inputs: Mapping[str, Scalar],
           entrypoint: Entrypoint | None = None, name: str | None = None,
           labels: Mapping[str, str] | None = None, keep_days: int | None = None,
           reader: Reader | None = None) -> dict:
    """An Argo Workflow that runs `pipeline` once with `inputs`.

    `legos` must contain the steps' tools (registry.check has passed on them). `entrypoint(image)`
    gives the ENTRYPOINT of images whose tool leaves `command` empty. `name` names the workflow (the
    caller's id for the run, e.g. a pygeoapi job id); without it Argo generates `<pipeline id>-xxxxx`.
    Argo copies the name into a pod label, so keep it a DNS name of 63 characters or less.
    `labels` are the caller's own workflow labels (e.g. who submitted the run); they can't replace
    hydrowf/pipeline or hydrowf/version.

    `reader` lets dataset ports read our bucket in place (Reader); without it such a pipeline is refused.

    Deleting the workflow deletes its output artifacts and archived logs too (artifactGC). With
    `keep_days`, Argo deletes the workflow, and so its outputs, that many days after it finishes.
    """
    if name == MANAGED_PREFIX.rstrip("/"):
        raise ValueError(f"a workflow can't be named {name!r}: its outputs would land under {MANAGED_PREFIX}")
    if keep_days is not None and (isinstance(keep_days, bool) or not isinstance(keep_days, int)
                                  or not 1 <= keep_days <= MAX_KEEP_DAYS):
        raise ValueError(f"keep_days={keep_days!r}: a whole number of days, 1 to {MAX_KEEP_DAYS}")
    extra = check_labels(labels or {})
    if reserved := sorted(set(extra) & set(RESERVED_LABELS)):
        raise ValueError(f"{', '.join(reserved)}: render() sets these labels itself")
    legos = list(legos)
    tools = {(x.id, x.version): x for x in legos if isinstance(x, ToolLego)}
    archives = output_archives(legos)
    data = {(x.id, x.version): x for x in legos if isinstance(x, DataLego)}
    values = pipeline_values(pipeline, inputs)
    steps = {s.name: s for s in pipeline.steps}
    templates, tasks = [], []
    reads: dict[str, list[str]] = {}                # step -> the keys it reads in place
    input_types = {port.name: port.type for port in pipeline.inputs}
    links_in_place = in_place_links(pipeline, tools)
    if links_in_place and not name:
        step, output = min(links_in_place)
        raise UnsupportedForArgo(f"reading step {step}'s output {output} in place needs the workflow's name "
                                 "(render's name): the reading step's argument and read keys name its key")
    linked: dict[tuple[str, str], str] = {}         # (step, output) -> its key, for the steps that read it
    for step_name in pipeline.order():
        step = steps[step_name]
        tool = tools.get(step.tool.key())
        if tool is None:
            raise UnsupportedForArgo(f"step {step_name}: {step.tool.id}@{step.tool.version} is not a registered tool")
        if "argo" not in tool.runs_on:
            raise UnsupportedForArgo(f"step {step_name}: {tool.id} does not run on argo")
        scalars, artifacts, step_reads = task_inputs(step, tool, values, data, reader, input_types, linked)
        ours = f"s3://{reader.bucket}/" if reader else ""
        if step_reads and any(isinstance(v, str) and v.startswith("s3://") and not v.startswith(ours)
                              for v in scalars.values()):
            # the step's S3 endpoint and keys are ours: an external s3:// source would go to our endpoint
            raise UnsupportedForArgo(f"step {step_name}: a step that reads our bucket in place can't also read "
                                     "another s3:// bucket")
        prefixes = {out: linked_prefix(name, step_name) for s, out in links_in_place if s == step_name}
        template = container_template(step, tool, scalars, artifacts, entrypoint, archives, reader, step_reads,
                                      prefixes)
        linked.update(linked_keys(template, step_name, prefixes))   # the keys the later steps will read
        templates.append(template)
        if step_reads:
            reads[step_name] = step_reads
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
                **({"hydrowf/reads": json.dumps(reads)} if reads else {}),   # step -> keys it reads in place
            },
        },
        "spec": {
            "entrypoint": "main",
            "artifactGC": {"strategy": "OnWorkflowDeletion"},
            **({"ttlStrategy": {"secondsAfterCompletion": keep_days * 86400}} if keep_days is not None else {}),
            "templates": [{"name": "main", "dag": {"tasks": tasks}}] + templates,
        },
    }
