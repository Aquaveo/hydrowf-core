"""Submit Argo Workflows and read their status and outputs (needs the `argo` extra: Hera's API client).

    runtime = ArgoRuntime("https://argo-server:2746", bucket="my-bucket")
    name = runtime.submit(render(pipeline, legos, inputs))
    runtime.status(name)      # RunStatus(phase="Running", steps={"preprocess": "Running"}, ...)
    runtime.outputs(name)     # {"run": "s3://my-bucket/<name>/<pod>/ngen-run.tgz"} once the step succeeded
    runtime.list("ngiab-simulation")   # [RunStatus, ...] of that pipeline's runs (all pipelines without it)
    runtime.delete(name)

The bucket is the cluster's artifact repository bucket: Argo records only the key of each output.
"""
from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime

from hera.exceptions import InternalServerError
from hera.workflows import WorkflowsService
from hera.workflows import models as m

from hydrowf_core.adapters.argo import check_labels

FINISHED = ("Succeeded", "Failed", "Error")
DONE = ("Succeeded", "Skipped", "Omitted")          # step phases that count toward progress


@dataclass(frozen=True)
class RunStatus:
    name: str
    phase: str                            # Pending, Running, Succeeded, Failed or Error
    message: str = ""                     # Argo's reason when a run fails
    steps: dict[str, str] = field(default_factory=dict)    # step (task) name -> phase
    pipeline: str = ""                    # the hydrowf/pipeline label render() sets
    created_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    progress: str = ""                    # "<done>/<total>" of the planned steps, e.g. "2/3"
    labels: dict[str, str] = field(default_factory=dict)   # the workflow's labels, render()'s and the caller's
    # pipeline output -> the data lego it was written as, "id@version" (render()'s hydrowf/formats); {} for
    # runs rendered before formats
    formats: dict[str, str] = field(default_factory=dict)
    # step -> the keys it reads in place (render()'s hydrowf/reads): what a step's read keys may cover
    reads: dict[str, list[str]] = field(default_factory=dict)
    uid: str = ""                         # the Workflow object's uid (its step pods' controlling owner)
    # step -> (the name of the pod Argo made for it, that node's phase): what a pod claiming to be a step
    # must match (Argo's pod naming v2: <workflow>-<template>-<node id's hash>)
    step_pods: dict[str, tuple[str, str]] = field(default_factory=dict)

    @property
    def finished(self) -> bool:
        return self.phase in FINISHED


class ArgoRuntime:
    def __init__(self, host: str, bucket: str, namespace: str = "argo", token: str | None = None,
                 verify_ssl: bool = True, service: WorkflowsService | None = None):
        self.bucket = bucket
        self.namespace = namespace
        self.service = service or WorkflowsService(host=host, namespace=namespace, token=token,
                                                   verify_ssl=verify_ssl)

    def submit(self, workflow: dict) -> str:
        """Create the workflow (render()'s dict) and return its generated name."""
        request = m.WorkflowCreateRequest(workflow=m.Workflow.model_validate(workflow))
        return self.service.create_workflow(request, namespace=self.namespace).metadata.name

    def _get(self, name: str) -> m.Workflow:
        return self.service.get_workflow(name=name, namespace=self.namespace)

    def status(self, name: str) -> RunStatus:
        return run_status(self._get(name))

    def list(self, pipeline: str | None = None, labels: Mapping[str, str] | None = None) -> list[RunStatus]:
        """The runs render() made (they carry the hydrowf/pipeline label), newest first; only
        `pipeline`'s runs when it is given, and only those carrying every one of `labels`."""
        selector = [f"hydrowf/pipeline={pipeline}" if pipeline else "hydrowf/pipeline"]
        selector += [f"{key}={value}" for key, value in check_labels(labels or {}).items()]
        workflow_list = self.service.list_workflows(namespace=self.namespace, label_selector=",".join(selector))
        runs = [run_status(workflow) for workflow in workflow_list.items or []]

        runs.sort(key=lambda run: (run.created_at is not None, run.created_at), reverse=True)
        return runs

    def delete(self, name: str) -> None:
        """Stop the workflow if it is still running, then delete it. Its pods go with it, and its
        artifacts too when render() wrote it: Argo deletes them afterwards, in a pod (artifactGC).
        If that fails, the workflow stays, being deleted, until its artifact-gc finalizer is removed.

        A run is terminated first because Argo collects artifacts only once a run has completed: a
        running workflow that is only deleted waits on its artifact-gc finalizer and keeps running,
        steps and all, until it ends on its own. Argo refuses to terminate a completed run (a 500),
        so a run that finishes between the status check and the terminate is deleted all the same.
        """
        if not self.status(name).finished:
            try:
                self.service.terminate_workflow(
                    name=name, req=m.WorkflowTerminateRequest(name=name, namespace=self.namespace),
                    namespace=self.namespace)
            except InternalServerError:
                if not self.status(name).finished:      # not the race: a real failure
                    raise
        self.service.delete_workflow(name=name, namespace=self.namespace)

    def outputs(self, name: str) -> dict[str, str]:
        """Pipeline output name -> s3://<bucket>/<key>, for each output whose step has succeeded.

        render() writes the annotation hydrowf/outputs = {"<output>": "<task>/<artifact>"}. Each Pod
        node's display_name is its task name, and node.outputs.artifacts lists what it uploaded
        (artifact.name, artifact.s3.key).
        """
        wf = self._get(name)
        wanted = outputs_of(wf.metadata.annotations or {})
        pods = {n.display_name: n for n in ((wf.status and wf.status.nodes) or {}).values()
                if n.type == "Pod" and n.phase == "Succeeded"}        # unfinished steps have no outputs yet
        found: dict[str, str] = {}
        for output, ref in wanted.items():
            task, artifact = ref.split("/", 1)
            node = pods.get(task)
            artifacts = (node.outputs.artifacts or []) if node and node.outputs else []
            match = next((a for a in artifacts if a.name == artifact and a.s3 and a.s3.key), None)
            if match:
                found[output] = f"s3://{self.bucket}/{match.s3.key}"
        return found


def time(t: m.Time | None) -> datetime | None:
    return t.root if t is not None else None


def plan(wf: m.Workflow) -> list[str]:
    """The task names of the workflow's entrypoint DAG: every step render() planned, including the
    ones Argo hasn't created a node for yet. [] for a workflow without a DAG entrypoint."""
    spec = wf.spec
    main = next((t for t in spec.templates or [] if t.name == spec.entrypoint), None) if spec else None
    return [task.name for task in main.dag.tasks] if main and main.dag else []


def progress(steps: dict[str, str], planned: list[str], argo_progress: str) -> str:
    """Done over planned steps, as "<done>/<total>"; Argo's own progress when there is no plan.

    `steps` is step name -> phase for the steps that have a node (run_status's `steps`). A step is
    done when it did its work (Succeeded) or a `when:` condition left it out (Skipped, Omitted);
    a Failed step is finished but not done.
    """
    if not planned:
        return argo_progress
    done = sum(steps.get(step) in DONE for step in planned)
    return f"{done}/{len(planned)}"


def outputs_of(annotations: dict[str, str]) -> dict[str, str]:
    """The hydrowf/outputs annotation as {output: "<task>/<artifact>"}; {} when it is missing or isn't that
    shape, like formats_of."""
    try:
        found = json.loads(annotations.get("hydrowf/outputs", "{}"))
    except ValueError:
        return {}
    if not isinstance(found, dict) or not all(isinstance(k, str) and isinstance(v, str) and "/" in v
                                              for k, v in found.items()):
        return {}
    return found


PLAIN_KEY = re.compile(r"[A-Za-z0-9._/-]+")


def plain_key(key: object) -> bool:
    return isinstance(key, str) and bool(PLAIN_KEY.fullmatch(key)) and all(
        part not in ("", ".", "..") for part in key.removesuffix("/").split("/"))


def reads_of(annotations: dict[str, str]) -> dict[str, list[str]]:
    """The hydrowf/reads annotation as {step: [keys]}; {} when it is missing or isn't that shape, or any key
    isn't a plain key (no empty, `.` or `..` part). Read keys are scoped to it, so it is checked here again:
    render writes it, but anyone who may edit workflows could change it."""
    try:
        found = json.loads(annotations.get("hydrowf/reads", "{}"))
    except ValueError:
        return {}
    if not isinstance(found, dict) or not all(
            isinstance(step, str) and isinstance(keys, list) and keys and all(plain_key(k) for k in keys)
            for step, keys in found.items()):
        return {}
    return found


def formats_of(annotations: dict[str, str]) -> dict[str, str]:
    """The hydrowf/formats annotation as {output: "id@version"}; {} when it is missing or isn't that shape
    (render() writes it, but anyone who may edit workflows could change it)."""
    try:
        found = json.loads(annotations.get("hydrowf/formats", "{}"))
    except ValueError:
        return {}
    if not isinstance(found, dict) or not all(isinstance(k, str) and isinstance(v, str) and "@" in v
                                              for k, v in found.items()):
        return {}
    return found


def step_pods(workflow: str, nodes: list) -> dict[str, tuple[str, str]]:
    """Step -> (pod name, phase) for the Pod nodes of `workflow`, by Argo's pod naming v2 (its default):
    <workflow>-<template>-<the node id after "<workflow>-">. Argo names a retry's or loop's pod nodes eval(0),
    eval(1): they are steps of their own here, which render records no reads for. Two Pod nodes with one
    display name would make the pod ambiguous: that step is left out (defensive; Argo doesn't make them)."""
    found: dict[str, tuple[str, str]] = {}
    seen: set[str] = set()
    for node in nodes:
        step, node_id = node.display_name, node.id or ""
        if not node_id.startswith(f"{workflow}-") or not node.template_name:
            continue
        if step in seen:
            found.pop(step, None)
            continue
        seen.add(step)
        found[step] = (f"{workflow}-{node.template_name}-{node_id[len(workflow) + 1:]}", node.phase or "Pending")
    return found


def run_status(wf: m.Workflow) -> RunStatus:
    """A RunStatus from one workflow (a get_workflow answer or a list_workflows item)."""
    meta, status = wf.metadata, wf.status
    labels = dict(meta.labels or {})
    common = dict(pipeline=labels.get("hydrowf/pipeline", ""), labels=labels, created_at=time(meta.creation_timestamp),
                  formats=formats_of(meta.annotations or {}), reads=reads_of(meta.annotations or {}),
                  uid=meta.uid or "")
    if status is None:                                   # the controller hasn't picked it up yet
        return RunStatus(meta.name, "Pending", **common)
    pods = [n for n in (status.nodes or {}).values() if n.type == "Pod"]
    steps = {n.display_name: n.phase or "Pending" for n in pods}
    return RunStatus(meta.name, status.phase or "Pending", status.message or "", steps, **common,
                     step_pods=step_pods(meta.name, pods),
                     started_at=time(status.started_at), finished_at=time(status.finished_at),
                     progress=progress(steps, plan(wf), status.progress or ""))
