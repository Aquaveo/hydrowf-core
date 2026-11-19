"""ArgoRuntime against a stub of Hera's WorkflowsService (no cluster).

The stub answers get_workflow with a response recorded from a real run on Argo v4.1.6
(ngiab-simulation-rgbl4, status fields only); tests edit a copy for other states.
"""
import copy
import json
from datetime import datetime, timezone
from pathlib import Path

import yaml
from hera.exceptions import InternalServerError
from hera.workflows import models as m

import pytest

from hydrowf_core.adapters.argo_runtime import ArgoRuntime, RunStatus

FIXTURES = Path(__file__).parent / "fixtures" / "argo"
SUCCEEDED = json.loads((FIXTURES / "status-ngiab-simulation-succeeded.json").read_text())
NAME = SUCCEEDED["metadata"]["name"]
LABELS = SUCCEEDED["metadata"]["labels"]          # what render() set on the recorded run


class StubService:
    """Answers get_workflow and list_workflows from recorded responses; keeps every create, list,
    terminate and delete request."""

    def __init__(self, workflow, listed=()):
        self.workflow = workflow
        self.listed = list(listed)
        self.created, self.list_calls, self.deleted, self.terminated = [], [], [], []
        self.on_terminate = None              # a test's stand-in for Argo answering a terminate

    def get_workflow(self, name, namespace=None):
        return m.Workflow.model_validate({"spec": {}, **self.workflow})    # the fixture keeps no spec

    def list_workflows(self, namespace=None, label_selector=None, **_):
        self.list_calls.append((namespace, label_selector))
        return m.WorkflowList(metadata=m.ListMeta(),
                              items=[m.Workflow.model_validate({"spec": {}, **w}) for w in self.listed])

    def delete_workflow(self, name, namespace=None, **_):
        self.deleted.append((name, namespace))

    def terminate_workflow(self, name, req, namespace=None):
        self.terminated.append((name, req.name, namespace))
        if self.on_terminate:
            self.on_terminate(self)

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


def with_plan(wf, *steps):
    """`wf` with the spec render() writes: an entrypoint DAG `main` with one task per step."""
    tasks = [{"name": step, "template": f"step-{step}"} for step in steps]
    return {**wf, "spec": {"entrypoint": "main", "templates": [{"name": "main", "dag": {"tasks": tasks}}]}}


def test_status_of_a_finished_run():
    status = runtime().status(NAME)
    assert status == RunStatus(NAME, "Succeeded", "", {"preprocess": "Succeeded", "run": "Succeeded"},
                               pipeline="ngiab-simulation", created_at=at("2026-11-15T00:29:58Z"),
                               started_at=at("2026-11-15T00:29:58Z"), finished_at=at("2026-11-15T00:40:15Z"),
                               progress="2/2", labels=LABELS)
    assert status.finished


def test_status_while_running():
    status = runtime(while_run_is("Running")).status(NAME)
    assert (status.phase, status.steps, status.finished) == (
        "Running", {"preprocess": "Succeeded", "run": "Running"}, False)


def test_progress_counts_every_planned_step():
    # Argo's own progress counts only the nodes it has created: "1/2" while to-cng doesn't exist yet
    wf = with_plan(while_run_is("Running"), "preprocess", "run", "to-cng")
    wf["status"]["progress"] = "1/2"
    assert runtime(wf).status(NAME).progress == "1/3"


def test_progress_of_a_finished_run_is_the_whole_plan():
    assert runtime(with_plan(SUCCEEDED, "preprocess", "run")).status(NAME).progress == "2/2"


def test_progress_after_a_failed_step():
    wf = with_plan(while_run_is("Failed"), "preprocess", "run", "to-cng")
    wf["status"]["phase"] = "Failed"
    assert runtime(wf).status(NAME).progress == "1/3"


def test_status_before_the_controller_sees_it():
    wf = {k: v for k, v in SUCCEEDED.items() if k != "status"}
    assert runtime(wf).status(NAME) == RunStatus(NAME, "Pending", pipeline="ngiab-simulation",
                                                 created_at=at("2026-11-15T00:29:58Z"), labels=LABELS)


def test_status_carries_the_callers_labels():
    wf = copy.deepcopy(SUCCEEDED)
    wf["metadata"]["labels"] = {**LABELS, "hydrowf/owner": "user-1"}
    assert runtime(wf).status(NAME).labels["hydrowf/owner"] == "user-1"


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


def test_list_of_one_callers_runs():
    rt = runtime(listed=older_and_newer())
    rt.list(labels={"hydrowf/owner": "8d3a6e2c-1f0b-4c4e-9a51-0e6d2b7c9f10"})
    assert rt.service.list_calls == [("argo", "hydrowf/pipeline,hydrowf/owner=8d3a6e2c-1f0b-4c4e-9a51-0e6d2b7c9f10")]


@pytest.mark.parametrize("value", ["a,hydrowf/owner", "x!=y", "a b", "-bad", "x" * 64])
def test_a_label_that_would_change_the_selector_is_refused(value):
    rt = runtime(listed=older_and_newer())
    with pytest.raises(ValueError, match="not a valid Kubernetes label"):
        rt.list(labels={"hydrowf/owner": value})
    assert rt.service.list_calls == []                                    # nothing reached Argo


def test_list_items_carry_their_status():
    [run] = runtime(listed=[SUCCEEDED]).list()
    assert run == runtime().status(NAME)


def test_deleting_a_finished_run_only_deletes_it():      # Argo refuses to terminate a completed run
    rt = runtime()
    rt.delete(NAME)
    assert rt.service.terminated == [] and rt.service.deleted == [(NAME, "argo")]


@pytest.mark.parametrize("phase", ["Pending", "Running"])
def test_an_unfinished_run_is_terminated_before_it_is_deleted(phase):
    rt = runtime(while_run_is(phase))       # else Argo keeps running it, held by the artifact-gc finalizer
    rt.delete(NAME)
    assert rt.service.terminated == [(NAME, NAME, "argo")] and rt.service.deleted == [(NAME, "argo")]


def test_a_run_that_finishes_before_the_terminate_is_still_deleted():
    def finish_then_refuse(service):        # Argo: "cannot shutdown a completed workflow", a 500
        service.workflow = copy.deepcopy(SUCCEEDED)
        raise InternalServerError("cannot shutdown a completed workflow")

    rt = runtime(while_run_is("Running"))
    rt.service.on_terminate = finish_then_refuse
    rt.delete(NAME)
    assert rt.service.deleted == [(NAME, "argo")]


def test_a_terminate_that_fails_for_a_running_run_keeps_it():
    def refuse(service):
        raise InternalServerError("etcd unavailable")

    rt = runtime(while_run_is("Running"))
    rt.service.on_terminate = refuse
    with pytest.raises(InternalServerError):
        rt.delete(NAME)
    assert rt.service.deleted == []          # not deleted half-stopped: the caller sees the error


def test_a_run_without_a_creation_time_sorts_last():
    untimed = copy.deepcopy(SUCCEEDED)
    untimed["metadata"]["name"] = "ngiab-simulation-untimed"
    del untimed["metadata"]["creationTimestamp"]
    assert [r.name for r in runtime(listed=[untimed, SUCCEEDED]).list()] == [NAME, "ngiab-simulation-untimed"]
