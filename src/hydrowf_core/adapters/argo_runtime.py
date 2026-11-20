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


def run_status(wf: m.Workflow) -> RunStatus:
    """A RunStatus from one workflow (a get_workflow answer or a list_workflows item)."""
    meta, status = wf.metadata, wf.status
    labels = dict(meta.labels or {})
    common = dict(pipeline=labels.get("hydrowf/pipeline", ""), labels=labels, created_at=time(meta.creation_timestamp),
                  formats=formats_of(meta.annotations or {}))
    if status is None:                                   # the controller hasn't picked it up yet
        return RunStatus(meta.name, "Pending", **common)
    steps = {n.display_name: n.phase or "Pending" for n in (status.nodes or {}).values() if n.type == "Pod"}
    return RunStatus(meta.name, status.phase or "Pending", status.message or "", steps, **common,
                     started_at=time(status.started_at), finished_at=time(status.finished_at),
                     progress=progress(steps, plan(wf), status.progress or ""))
