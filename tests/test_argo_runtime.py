"""ArgoRuntime against a stub of Hera's WorkflowsService (no cluster).

The stub answers get_workflow with a response recorded from a real run on Argo v4.1.6
(ngiab-simulation-rgbl4, status fields only); tests edit a copy for other states.
"""
import copy
import json
from pathlib import Path

import yaml
from hera.workflows import models as m

from hydrowf_core.adapters.argo_runtime import ArgoRuntime, RunStatus

FIXTURES = Path(__file__).parent / "fixtures" / "argo"
SUCCEEDED = json.loads((FIXTURES / "status-ngiab-simulation-succeeded.json").read_text())
NAME = SUCCEEDED["metadata"]["name"]


class StubService:
    """Answers get_workflow from a recorded response; keeps every create request."""

    def __init__(self, workflow):
        self.workflow = workflow
        self.created = []

    def get_workflow(self, name, namespace=None):
        return m.Workflow.model_validate({"spec": {}, **self.workflow})    # the fixture keeps no spec

    def create_workflow(self, req, namespace=None):
        self.created.append((req, namespace))
        return m.Workflow(metadata=m.ObjectMeta(name=req.workflow.metadata.generate_name + "abc12"),
                          spec=req.workflow.spec)


def runtime(workflow=SUCCEEDED):
    return ArgoRuntime("https://unused", bucket="my-bucket", service=StubService(copy.deepcopy(workflow)))


def while_run_is(phase):
    """The recorded run, as it looked while its `run` step was still in `phase`."""
    wf = copy.deepcopy(SUCCEEDED)
    wf["status"]["phase"] = "Running"
    for node in wf["status"]["nodes"].values():
        if node["displayName"] in ("run", NAME):
            node["phase"] = phase
            node.pop("outputs", None)
    return wf


def test_submit_sends_the_rendered_workflow_unchanged():
    workflow = yaml.safe_load((FIXTURES / "ngiab-simulation.yaml").read_text())
    rt = runtime()
    assert rt.submit(workflow) == "ngiab-simulation-abc12"
    [(request, namespace)] = rt.service.created
    assert namespace == "argo"
    assert request.workflow.model_dump(mode="json", by_alias=True, exclude_none=True) == workflow


def test_status_of_a_finished_run():
    status = runtime().status(NAME)
    assert status == RunStatus(NAME, "Succeeded", "", {"preprocess": "Succeeded", "run": "Succeeded"})
    assert status.finished


def test_status_while_running():
    status = runtime(while_run_is("Running")).status(NAME)
    assert (status.phase, status.steps, status.finished) == (
        "Running", {"preprocess": "Succeeded", "run": "Running"}, False)


def test_status_before_the_controller_sees_it():
    wf = {k: v for k, v in SUCCEEDED.items() if k != "status"}
    assert runtime(wf).status(NAME) == RunStatus(NAME, "Pending")


def test_outputs_of_a_finished_run():
    assert runtime().outputs(NAME) == {
        "run": f"s3://my-bucket/{NAME}/{NAME}-step-run-432766967/ngen-run.tgz"}


def test_no_outputs_until_their_step_succeeds():
    assert runtime(while_run_is("Running")).outputs(NAME) == {}


def test_no_outputs_without_the_annotation():
    wf = copy.deepcopy(SUCCEEDED)
    wf["metadata"]["annotations"] = {}
    assert runtime(wf).outputs(NAME) == {}
