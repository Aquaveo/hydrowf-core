"""ArgoRuntime against a stub of Hera's WorkflowsService (no cluster).

The stub answers get_workflow with a response recorded from a real run on Argo v4.1.6
(ngiab-simulation-rgbl4, status fields only); tests edit a copy for other states.
"""
import copy
import json
from datetime import datetime, timezone
from pathlib import Path

import yaml
from hera.workflows import models as m

from hydrowf_core.adapters.argo_runtime import ArgoRuntime, RunStatus

FIXTURES = Path(__file__).parent / "fixtures" / "argo"
SUCCEEDED = json.loads((FIXTURES / "status-ngiab-simulation-succeeded.json").read_text())
NAME = SUCCEEDED["metadata"]["name"]


class StubService:
    """Answers get_workflow and list_workflows from recorded responses; keeps every create, list and
    delete request."""

    def __init__(self, workflow, listed=()):
        self.workflow = workflow
        self.listed = list(listed)
        self.created, self.list_calls, self.deleted = [], [], []

    def get_workflow(self, name, namespace=None):
        return m.Workflow.model_validate({"spec": {}, **self.workflow})    # the fixture keeps no spec

    def list_workflows(self, namespace=None, label_selector=None, **_):
        self.list_calls.append((namespace, label_selector))
        return m.WorkflowList(metadata=m.ListMeta(),
                              items=[m.Workflow.model_validate({"spec": {}, **w}) for w in self.listed])

    def delete_workflow(self, name, namespace=None, **_):
        self.deleted.append((name, namespace))

    def create_workflow(self, req, namespace=None):
        self.created.append((req, namespace))
        return m.Workflow(metadata=m.ObjectMeta(name=req.workflow.metadata.generate_name + "abc12"),
                          spec=req.workflow.spec)


def runtime(workflow=SUCCEEDED, listed=()):
    return ArgoRuntime("https://unused", bucket="my-bucket",
                       service=StubService(copy.deepcopy(workflow), copy.deepcopy(list(listed))))


def at(text):
    return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)


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
    assert status == RunStatus(NAME, "Succeeded", "", {"preprocess": "Succeeded", "run": "Succeeded"},
                               pipeline="ngiab-simulation", created_at=at("2026-11-15T00:29:58Z"),
                               started_at=at("2026-11-15T00:29:58Z"), finished_at=at("2026-11-15T00:40:15Z"),
                               progress="2/2")
    assert status.finished


def test_status_while_running():
    status = runtime(while_run_is("Running")).status(NAME)
    assert (status.phase, status.steps, status.finished) == (
        "Running", {"preprocess": "Succeeded", "run": "Running"}, False)


def test_status_before_the_controller_sees_it():
    wf = {k: v for k, v in SUCCEEDED.items() if k != "status"}
    assert runtime(wf).status(NAME) == RunStatus(NAME, "Pending", pipeline="ngiab-simulation",
                                                 created_at=at("2026-11-15T00:29:58Z"))


def test_outputs_of_a_finished_run():
    assert runtime().outputs(NAME) == {
        "run": f"s3://my-bucket/{NAME}/{NAME}-step-run-432766967/ngen-run.tgz"}


def test_no_outputs_until_their_step_succeeds():
    assert runtime(while_run_is("Running")).outputs(NAME) == {}


def test_no_outputs_without_the_annotation():
    wf = copy.deepcopy(SUCCEEDED)
    wf["metadata"]["annotations"] = {}
    assert runtime(wf).outputs(NAME) == {}


def older_and_newer():
    """Two recorded runs: the real one, and a copy created an hour later."""
    newer = copy.deepcopy(SUCCEEDED)
    newer["metadata"]["name"] = "ngiab-simulation-newer"
    newer["metadata"]["creationTimestamp"] = "2026-11-15T01:29:58Z"
    return [SUCCEEDED, newer]


def test_list_returns_every_hydrowf_run_newest_first():
    rt = runtime(listed=older_and_newer())
    assert [r.name for r in rt.list()] == ["ngiab-simulation-newer", NAME]
    assert rt.service.list_calls == [("argo", "hydrowf/pipeline")]      # label exists: render() made it


def test_list_of_one_pipeline():
    rt = runtime(listed=older_and_newer())
    rt.list("ngiab-simulation")
    assert rt.service.list_calls == [("argo", "hydrowf/pipeline=ngiab-simulation")]


def test_list_items_carry_their_status():
    [run] = runtime(listed=[SUCCEEDED]).list()
    assert run == runtime().status(NAME)


def test_delete_removes_the_workflow():
    rt = runtime()
    rt.delete(NAME)
    assert rt.service.deleted == [(NAME, "argo")]


def test_a_run_without_a_creation_time_sorts_last():
    untimed = copy.deepcopy(SUCCEEDED)
    untimed["metadata"]["name"] = "ngiab-simulation-untimed"
    del untimed["metadata"]["creationTimestamp"]
    assert [r.name for r in runtime(listed=[untimed, SUCCEEDED]).list()] == [NAME, "ngiab-simulation-untimed"]
