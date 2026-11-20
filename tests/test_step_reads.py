"""Dataset ports that read our bucket in place: the step gets read keys from the vendor (Reader)."""
import json

import pytest
from test_data_sources import pipeline_of, registry

from hydrowf_core.adapters.argo import READS_DIR, BadInputs, Reader, UnsupportedForArgo, render
from hydrowf_core.models import LEGOS, Pipeline
from hydrowf_core.registry import check

CURL = "curlimages/curl@sha256:" + "a" * 64
READER = Reader(image=CURL, vendor_url="http://hydrowf-ogc.hydrowf.svc:8001/step-keys", bucket="my-bucket",
                s3_endpoint="http://minio.argo.svc:9000")
LOCATION = "managed/conus-gpkg/2.2.0/conus_nextgen.gpkg"
JOB_OUTPUT = "s3://my-bucket/job-1/job-1-step-to-cng-1/cng/"


def step_template(workflow):
    return next(t for t in workflow["spec"]["templates"] if t["name"] == "step-read")


def managed_on_a_dataset_port():
    return registry("dataset", "conus-gpkg")


def caller_dataset_input():
    legos = registry("dataset", "nwm-retro", network=True)
    raw = pipeline_of(legos).model_dump(mode="json")
    raw["inputs"] = [{"name": "ds", "type": "dataset", "data": "nwm-retro"}]
    raw["steps"][0]["inputs"]["src"] = {"input": "ds"}                 # the caller's s3:// key, not the URL
    return Pipeline.model_validate(raw), legos


# ---- the registry


def test_managed_data_may_feed_a_dataset_port():
    assert check(managed_on_a_dataset_port()) == []


# ---- rendering a step that reads in place


def test_managed_data_on_a_dataset_port_is_read_in_place():
    legos = managed_on_a_dataset_port()
    workflow = render(pipeline_of(legos), legos, {}, reader=READER)
    step = step_template(workflow)
    assert step["container"]["args"] == [f"s3://my-bucket/{LOCATION}"]
    assert "arguments" not in workflow["spec"]["templates"][0]["dag"]["tasks"][0]       # nothing staged
    assert json.loads(workflow["metadata"]["annotations"]["hydrowf/reads"]) == {"read": [LOCATION]}


def test_a_callers_job_output_on_a_dataset_port_is_read_in_place():
    pipeline, legos = caller_dataset_input()
    workflow = render(pipeline, legos, {"ds": JOB_OUTPUT}, reader=READER)
    assert step_template(workflow)["container"]["args"] == [JOB_OUTPUT]
    reads = json.loads(workflow["metadata"]["annotations"]["hydrowf/reads"])
    assert reads == {"read": ["job-1/job-1-step-to-cng-1/cng/"]}


def test_the_step_fetches_its_keys_before_the_tool_starts():
    legos = managed_on_a_dataset_port()
    step = step_template(render(pipeline_of(legos), legos, {}, reader=READER))
    [init] = step["initContainers"]
    assert init["image"] == CURL
    assert init["command"][0] == "curl" and init["command"][-1] == READER.vendor_url
    assert "--retry-connrefused" in init["command"] and init["resources"]["limits"] == {"memory": "64Mi"}
    assert f"@{READS_DIR}/token/token" in init["command"]                      # the token is the body
    assert not any("$(" in word or "{{" in word for word in init["command"])   # nothing gets expanded
    volumes = {v["name"]: v for v in step["volumes"]}
    token = volumes["hydrowf-token"]["projected"]["sources"][0]["serviceAccountToken"]
    assert token == {"audience": "hydrowf-vendor", "expirationSeconds": 600, "path": "token"}
    assert volumes["hydrowf-aws"] == {"name": "hydrowf-aws", "emptyDir": {"medium": "Memory"}}


def tool_with_env(legos, env):
    tool = next(x for x in legos if x.id == "reader").model_copy(update={"env": env})
    return [tool if x.id == "reader" else x for x in legos]


def test_the_tool_finds_the_keys_and_cannot_change_them():
    legos = tool_with_env(managed_on_a_dataset_port(), {"AWS_ENDPOINT_URL": "http://evil.example.org", "OTHER": "x"})
    container = step_template(render(pipeline_of(legos), legos, {}, reader=READER))["container"]
    env = {e["name"]: e["value"] for e in container["env"]}
    assert env["AWS_SHARED_CREDENTIALS_FILE"] == env["CPL_AWS_CREDENTIALS_FILE"] == f"{READS_DIR}/aws/credentials"
    assert env["AWS_ENDPOINT_URL"] == env["AWS_ENDPOINT_URL_S3"] == READER.s3_endpoint and env["OTHER"] == "x"
    assert env["AWS_PROFILE"] == "default" and env["AWS_ACCESS_KEY_ID"] == env["AWS_SESSION_TOKEN"] == ""
    assert (env["AWS_S3_ENDPOINT"], env["AWS_HTTPS"]) == ("minio.argo.svc:9000", "NO")      # GDAL's form
    assert [e["name"] for e in container["env"]].count("AWS_ENDPOINT_URL") == 1
    assert {"name": "hydrowf-aws", "mountPath": f"{READS_DIR}/aws", "readOnly": True} in container["volumeMounts"]
    assert not any(m["name"] == "hydrowf-token" for m in container["volumeMounts"])   # no token for the tool


@pytest.mark.parametrize("name", ["AWS_ACCESS_KEY_ID", "AWS_PROFILE", "AWS_ENDPOINT_URL_S3", "AWS_CONFIG_FILE",
                                  "AWS_ANYTHING"])
def test_a_tool_sets_no_aws_variable_in_a_step_that_reads_with_vended_keys(name):
    legos = tool_with_env(managed_on_a_dataset_port(), {name: "http://evil.example.org"})
    container = step_template(render(pipeline_of(legos), legos, {}, reader=READER))["container"]
    assert "http://evil.example.org" not in [e["value"] for e in container["env"]]


def test_a_tools_aws_variables_stay_in_steps_that_read_nothing_in_place():
    legos = tool_with_env(registry("file", "conus-gpkg"), {"AWS_PROFILE": "mine"})
    container = step_template(render(pipeline_of(legos), legos, {}, reader=READER))["container"]
    assert container["env"] == [{"name": "AWS_PROFILE", "value": "mine"}]


def test_without_a_reader_reading_in_place_is_refused():
    legos = managed_on_a_dataset_port()
    with pytest.raises(UnsupportedForArgo, match="needs read keys for the step"):
        render(pipeline_of(legos), legos, {})


def test_steps_that_read_nothing_in_place_get_no_keys():
    legos = registry("file", "conus-gpkg")                    # staged, not read in place
    workflow = render(pipeline_of(legos), legos, {}, reader=READER)
    assert "initContainers" not in step_template(workflow)
    assert "hydrowf/reads" not in workflow["metadata"]["annotations"]


def test_external_data_needs_no_keys():
    legos = registry("dataset", "nwm-retro", network=True)
    workflow = render(pipeline_of(legos), legos, {}, reader=READER)
    assert "initContainers" not in step_template(workflow)


# ---- the Reader itself


@pytest.mark.parametrize("change, message", [
    ({"image": "curlimages/curl:8.11.0"}, "pinned by digest"),
    ({"image": "curl{{x}}@sha256:" + "a" * 64}, "pinned by digest"),
    ({"vendor_url": "http://x/{{workflow.name}}"}, "plain http"),
    ({"vendor_url": "http://x/$(HOME)"}, "plain http"),
    ({"s3_endpoint": "ftp://minio"}, "plain http"),
    ({"bucket": "My_Bucket"}, "bucket name"),
    ({"token_seconds": 60}, "600 to 86400"),
])
def test_a_reader_holds_nothing_argo_or_kubernetes_would_expand(change, message):
    fields = {**READER.__dict__, **change}
    with pytest.raises(ValueError, match=message):
        Reader(**fields)


# ---- what a caller's dataset input may read: our bucket, a plain key, a dataset input only


@pytest.mark.parametrize("url, error", [
    ("s3://my-bucket/", "not a plain key"),                                  # the whole bucket
    ("s3://other-bucket/job-1/job-1-step-x-1/cng/", "s3://my-bucket/<key>"),   # not moved onto ours
    ("s3://my-bucket/job-1/../job-2/x/", "not a plain key"),
    ("s3://my-bucket/job-1//x/", "not a plain key"),
    ("s3://my-bucket/job 1/x/", "not a plain key"),
    ("s3://my-bucket/{{workflow.name}}/x/", "{, }, \\$"),
], ids=["whole-bucket", "other-bucket", "dotdot", "empty-part", "space", "template"])
def test_a_callers_dataset_key_is_checked_before_it_becomes_a_read(url, error):
    pipeline, legos = caller_dataset_input()
    with pytest.raises(BadInputs, match=error):
        render(pipeline, legos, {"ds": url}, reader=READER)


def test_a_callers_dataset_input_without_a_reader_is_refused():
    pipeline, legos = caller_dataset_input()
    with pytest.raises(UnsupportedForArgo, match="needs read keys"):
        render(pipeline, legos, {"ds": JOB_OUTPUT})


def test_only_a_dataset_input_feeds_a_dataset_port():
    pipeline, legos = caller_dataset_input()                   # check() refuses this; render must too
    raw = pipeline.model_dump(mode="json")
    raw["inputs"] = [{"name": "ds", "type": "string"}]
    with pytest.raises(UnsupportedForArgo, match="reads only from a dataset input"):
        render(Pipeline.model_validate(raw), legos, {"ds": JOB_OUTPUT}, reader=READER)


def test_a_constant_never_feeds_a_dataset_port():
    pipeline, legos = caller_dataset_input()
    raw = pipeline.model_dump(mode="json")
    raw["inputs"] = []
    raw["steps"][0]["inputs"]["src"] = {"value": JOB_OUTPUT}
    with pytest.raises(UnsupportedForArgo, match="not a constant"):
        render(Pipeline.model_validate(raw), legos, {}, reader=READER)


def test_two_reads_in_one_step_and_reads_per_step():
    pipeline, legos = caller_dataset_input()
    tool = next(x for x in legos if x.id == "reader")
    raw_tool = tool.model_dump(mode="json")
    raw_tool["id"] = "reader2"
    raw_tool["inputs"].append({"name": "hf", "type": "dataset", "data": "conus-gpkg"})
    raw_tool["args"] = ["{src}", "{hf}"]
    raw = pipeline.model_dump(mode="json")
    raw["steps"] = [{**raw["steps"][0], "tool": {"id": "reader2", "version": "0.1.0"},
                     "inputs": {"src": {"input": "ds"}, "hf": {"data": {"id": "conus-gpkg", "version": "2.2.0"}}}},
                    {**raw["steps"][0], "name": "again"}]
    raw["outputs"] = {"out": {"step": "read", "output": "out"}}
    legos = legos + [type(tool).model_validate(raw_tool)]
    workflow = render(Pipeline.model_validate(raw), legos, {"ds": JOB_OUTPUT}, reader=READER)
    reads = json.loads(workflow["metadata"]["annotations"]["hydrowf/reads"])
    job = "job-1/job-1-step-to-cng-1/cng/"
    assert reads == {"read": [job, LOCATION], "again": [job]}           # in the tool's port order


def test_a_pipeline_that_reads_nothing_in_place_renders_the_same_with_a_reader():
    legos = registry("file", "conus-gpkg")
    assert render(pipeline_of(legos), legos, {}, reader=READER) == render(pipeline_of(legos), legos, {})


@pytest.mark.parametrize("image", ["localhost:5001/curl@sha256:" + "b" * 64, "ghcr.io/org/curl@sha256:" + "b" * 64])
def test_a_reader_image_may_come_from_a_registry_with_a_port(image):
    Reader(**{**READER.__dict__, "image": image, "s3_endpoint": "https://s3.us-east-1.amazonaws.com"})


def test_https_s3_tells_gdal_to_use_https():
    reader = Reader(**{**READER.__dict__, "s3_endpoint": "https://s3.us-east-1.amazonaws.com"})
    legos = managed_on_a_dataset_port()
    env = {e["name"]: e["value"] for e in step_template(render(pipeline_of(legos), legos, {}, reader=reader))
           ["container"]["env"]}
    assert (env["AWS_S3_ENDPOINT"], env["AWS_HTTPS"]) == ("s3.us-east-1.amazonaws.com", "YES")


def test_a_step_that_reads_in_place_cannot_also_read_another_s3_bucket():
    pipeline, legos = caller_dataset_input()
    tool = next(x for x in legos if x.id == "reader").model_dump(mode="json")
    tool.update(id="reader3", network=True, args=["{src}", "{nwm}"])
    tool["inputs"].append({"name": "nwm", "type": "dataset", "data": "nwm-retro"})
    external = next(x for x in legos if x.id == "nwm-retro").model_dump(mode="json")
    external["storage"]["url"] = "s3://noaa-nwm-retrospective-3-0-pds/CONUS/zarr/chrtout.zarr"
    raw = pipeline.model_dump(mode="json")
    raw["steps"][0]["tool"] = {"id": "reader3", "version": "0.1.0"}
    raw["steps"][0]["inputs"]["nwm"] = {"data": {"id": "nwm-retro", "version": "1.0.0"}}
    legos = [x for x in legos if x.id != "nwm-retro"] + LEGOS.validate_python([tool, external])
    with pytest.raises(UnsupportedForArgo, match="can't also read another s3:// bucket"):
        render(Pipeline.model_validate(raw), legos, {"ds": JOB_OUTPUT}, reader=READER)
