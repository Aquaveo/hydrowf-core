"""Submit Argo Workflows and read their status and outputs (needs the `argo` extra: Hera's API client).

    runtime = ArgoRuntime("https://argo-server:2746", bucket="my-bucket")
    name = runtime.submit(render(pipeline, legos, inputs))
    runtime.status(name)      # RunStatus(phase="Running", steps={"preprocess": "Running"}, ...)
    runtime.outputs(name)     # {"run": "s3://my-bucket/<name>/<pod>/ngen-run.tgz"} once the step succeeded

The bucket is the cluster's artifact repository bucket: Argo records only the key of each output.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field

from hera.workflows import WorkflowsService
from hera.workflows import models as m

FINISHED = ("Succeeded", "Failed", "Error")


@dataclass(frozen=True)
class RunStatus:
    name: str
    phase: str                            # Pending, Running, Succeeded, Failed or Error
    message: str = ""                     # Argo's reason when a run fails
    steps: dict[str, str] = field(default_factory=dict)    # step (task) name -> phase

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
        status = self._get(name).status
        if status is None:                                   # the controller hasn't picked it up yet
            return RunStatus(name, "Pending")
        steps = {n.display_name: n.phase or "Pending" for n in (status.nodes or {}).values() if n.type == "Pod"}
        return RunStatus(name, status.phase or "Pending", status.message or "", steps)

    def outputs(self, name: str) -> dict[str, str]:
        """Pipeline output name -> s3://<bucket>/<key>, for each output whose step has succeeded.

        render() writes the annotation hydrowf/outputs = {"<output>": "<task>/<artifact>"}. Each Pod
        node's display_name is its task name, and node.outputs.artifacts lists what it uploaded
        (artifact.name, artifact.s3.key).
        """
        wf = self._get(name)
        wanted = json.loads((wf.metadata.annotations or {}).get("hydrowf/outputs", "{}"))
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
