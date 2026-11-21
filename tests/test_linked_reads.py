"""A step's output read in place by a later step of the same run (docs/design/formats-and-data-inputs.md D.7):
the link feeds a dataset port, the upstream output gets a key render knows (<run>/<step>/<output>), and that
key is what the later step's read keys cover."""
import json

import pytest

from hydrowf_core.adapters.argo import BadInputs, Reader, UnsupportedForArgo, render
from hydrowf_core.models import LEGOS, Pipeline
from hydrowf_core.registry import check

CURL = "curlimages/curl@sha256:" + "a" * 64
READER = Reader(image=CURL, vendor_url="http://hydrowf-ogc.hydrowf.svc:8001/step-keys", bucket="my-bucket",
                s3_endpoint="http://minio.argo.svc:9000")


def legos(feeds="out", series_archive="none", port_data="series"):
    return LEGOS.validate_python(raw(feeds, series_archive, port_data))


def raw(feeds="out", series_archive="none", port_data="series"):
    """A writer whose output `feeds` (out: a directory, single: a file, both of data `series`) feeds the
    evaluator's dataset port; the writer's `log` output is read by nobody. As dicts, for tests to change."""
    data = [{"kind": "data", "id": i, "version": "1.0.0", "title": i, "data_kind": "output",
             "storage": {"driver": "stac", "format": "other", "archive": a}}
            for i, a in (("series", series_archive), ("other", "none"))]
    writer = {"kind": "tool", "id": "writer", "version": "0.1.0", "title": "writer", "image": "w:1",
              "command": ["write"], "args": ["{workdir}"],
              "outputs": [{"name": "out", "type": "directory", "data": "series", "path": "out"},
                          {"name": "single", "type": "file", "data": "series", "path": "single/data.parquet"},
                          {"name": "log", "type": "file", "data": "other", "path": "log.txt"}]}
    evaluator = {"kind": "tool", "id": "evaluator", "version": "0.1.0", "title": "evaluator", "image": "e:1",
                 "command": ["evaluate"], "args": ["{ts}", "{workdir}/res"],
                 "inputs": [{"name": "ts", "type": "dataset", "data": port_data}],
                 "outputs": [{"name": "res", "type": "directory", "data": "other", "path": "res"}]}
    pipeline = {"kind": "pipeline", "id": "p", "version": "0.1.0", "title": "p",
                "steps": [{"name": "make", "tool": {"id": "writer", "version": "0.1.0"}},
                          {"name": "eval", "tool": {"id": "evaluator", "version": "0.1.0"},
                           "inputs": {"ts": {"step": "make", "output": feeds}}}],
                "outputs": {"res": {"step": "eval", "output": "res"}, "log": {"step": "make", "output": "log"}}}
    return data + [writer, evaluator, pipeline]


def lego(found, lego_id):
    return next(x for x in found if x.get("id") == lego_id)


def rendered(found, **kw):
    pipeline = next(x for x in found if isinstance(x, Pipeline))
    return render(pipeline, found, {}, **{"name": "job-1", "reader": READER, **kw})


def template(workflow, step):
    return next(t for t in workflow["spec"]["templates"] if t["name"] == f"step-{step}")


def task(workflow, step):
    return next(t for t in workflow["spec"]["templates"][0]["dag"]["tasks"] if t["name"] == step)


def output_key(workflow, step, output):
    return next(a for a in template(workflow, step)["outputs"]["artifacts"] if a["name"] == output)["s3"]["key"]


# ---- the registry


def test_a_steps_directory_output_may_feed_a_later_dataset_port_of_its_data():
    assert check(legos()) == []


def test_a_steps_file_output_may_feed_a_later_dataset_port_of_its_data():
    assert check(legos(feeds="single")) == []


def test_another_data_lego_is_still_refused():
    [problem] = check(legos(port_data="other"))
    assert "make.out is directory of series, the port wants dataset of other" in problem


def test_an_output_stored_as_one_tgz_cant_be_read_in_place():
    [problem] = check(legos(series_archive="tgz"))
    assert "make.out" in problem and "one .tgz" in problem


# ---- rendering


def test_the_later_step_reads_the_output_in_place_from_a_key_render_knows():
    workflow = rendered(legos())
    assert output_key(workflow, "make", "out") == "job-1/make/out/"
    assert template(workflow, "eval")["container"]["args"] == ["s3://my-bucket/job-1/make/out/", "/work/res"]
    assert json.loads(workflow["metadata"]["annotations"]["hydrowf/reads"]) == {"eval": ["job-1/make/out/"]}
    assert "arguments" not in task(workflow, "eval")                           # nothing staged
    assert task(workflow, "eval")["dependencies"] == ["make"]                   # still runs after it
    assert template(workflow, "eval")["initContainers"][0]["image"] == CURL     # and fetches its keys


def test_a_file_output_keeps_its_file_name_under_its_own_folder():
    workflow = rendered(legos(feeds="single"))
    assert output_key(workflow, "make", "single") == "job-1/make/single/data.parquet"
    assert json.loads(workflow["metadata"]["annotations"]["hydrowf/reads"]) == {
        "eval": ["job-1/make/single/data.parquet"]}


def test_outputs_no_step_reads_in_place_keep_argos_key():
    workflow = rendered(legos())
    assert output_key(workflow, "make", "log") == "{{workflow.name}}/{{pod.name}}/log/log.txt"
    assert output_key(workflow, "eval", "res") == "{{workflow.name}}/{{pod.name}}/res/"


def test_reading_a_steps_output_in_place_needs_the_workflows_name():
    with pytest.raises(UnsupportedForArgo, match="needs the workflow's name"):
        rendered(legos(), name=None)


def test_a_dataset_output_cant_be_linked():
    found = raw()
    lego(found, "writer")["outputs"][0] = {"name": "out", "type": "dataset", "data": "series"}
    [problem] = check(LEGOS.validate_python(found))
    assert "make.out is a dataset output: only file and directory outputs can be linked" in problem


def test_the_format_the_step_chose_is_the_one_read_in_place():
    found = raw()
    lego(found, "writer")["outputs"][0] = {"name": "out", "type": "directory", "formats": ["other", "series"],
                                           "path": "out"}
    lego(found, "writer")["args"] = ["{workdir}", "{out_format}"]
    lego(found, "p")["steps"][0]["formats"] = {"out": "series"}
    assert check(LEGOS.validate_python(found)) == []
    lego(found, "p")["steps"][0]["formats"] = {"out": "other"}
    [problem] = check(LEGOS.validate_python(found))
    assert "make.out is directory of other, the port wants dataset of series" in problem


def test_an_output_staged_into_one_step_and_read_in_place_by_another():
    found = raw()
    lego(found, "p")["steps"].append({"name": "copy", "tool": {"id": "copier", "version": "0.1.0"},
                                      "inputs": {"src": {"step": "make", "output": "out"}}})
    found.append({"kind": "tool", "id": "copier", "version": "0.1.0", "title": "copier", "image": "c:1",
                  "command": ["copy"], "args": ["{src}"],
                  "inputs": [{"name": "src", "type": "directory", "data": "series"}]})
    assert check(LEGOS.validate_python(found)) == []
    workflow = rendered(LEGOS.validate_python(found))
    assert output_key(workflow, "make", "out") == "job-1/make/out/"
    [staged] = task(workflow, "copy")["arguments"]["artifacts"]     # Argo finds the key in the node's record
    assert staged == {"name": "src", "from": "{{tasks.make.outputs.artifacts.out}}"}
    assert json.loads(workflow["metadata"]["annotations"]["hydrowf/reads"]) == {"eval": ["job-1/make/out/"]}


def test_two_steps_reading_one_output_read_the_same_key():
    found = raw()
    second = dict(lego(found, "p")["steps"][1], name="eval-again")
    lego(found, "p")["steps"].append(second)
    workflow = rendered(LEGOS.validate_python(found))
    assert json.loads(workflow["metadata"]["annotations"]["hydrowf/reads"]) == {
        "eval": ["job-1/make/out/"], "eval-again": ["job-1/make/out/"]}


def test_render_refuses_an_output_whose_newest_data_lego_packs_it_into_a_tgz():
    found = raw()       # check passed on 1.0.0; a 2.0.0 that packs outputs is published before the run
    found.append(dict(lego(found, "series"), version="2.0.0",
                      storage={"driver": "stac", "format": "other", "archive": "tgz"}))
    with pytest.raises(UnsupportedForArgo, match="stored as one .tgz: it can't be read in place"):
        rendered(LEGOS.validate_python(found))


def test_a_file_name_a_callers_input_makes_must_be_a_plain_key():
    found = raw(feeds="single")
    lego(found, "writer")["inputs"] = [{"name": "fname", "type": "string"}]
    lego(found, "writer")["outputs"][1]["path"] = "single/{fname}"
    lego(found, "p")["inputs"] = [{"name": "fname", "type": "string"}]
    lego(found, "p")["steps"][0]["inputs"] = {"fname": {"input": "fname"}}
    found = LEGOS.validate_python(found)
    pipeline = next(x for x in found if isinstance(x, Pipeline))
    workflow = render(pipeline, found, {"fname": "data.parquet"}, name="job-1", reader=READER)
    assert output_key(workflow, "make", "single") == "job-1/make/single/data.parquet"
    with pytest.raises(BadInputs, match="is not a plain key"):
        render(pipeline, found, {"fname": "my data.parquet"}, name="job-1", reader=READER)


def test_reading_a_steps_output_in_place_needs_a_reader():
    with pytest.raises(UnsupportedForArgo, match="needs read keys for the step"):
        rendered(legos(), reader=None)
