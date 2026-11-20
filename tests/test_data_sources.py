"""Registered data legos as step inputs: external URLs, managed STAC data at its location, Iceberg (not yet)."""
import copy
import re

import pytest
from pydantic import ValidationError

from hydrowf_core.adapters.argo import UnsupportedForArgo, render
from hydrowf_core.models import LEGOS, Pipeline, Port
from hydrowf_core.registry import check

URL = "https://noaa-nwm-retrospective-3-0-pds.s3.amazonaws.com/CONUS/zarr/chrtout.zarr"
EXTERNAL = {"kind": "data", "id": "nwm-retro", "version": "1.0.0", "title": "NWM retrospective",
            "data_kind": "external", "storage": {"driver": "external", "url": URL, "format": "zarr"}}
MANAGED = {"kind": "data", "id": "conus-gpkg", "version": "2.2.0", "title": "CONUS hydrofabric",
           "data_kind": "managed", "storage": {"driver": "stac", "format": "other",
                                               "location": "managed/conus-gpkg/2.2.0/conus_nextgen.gpkg"}}
ICEBERG = {"kind": "data", "id": "hf-tables", "version": "1.0.0", "title": "Hydrofabric tables",
           "data_kind": "managed", "storage": {"driver": "iceberg", "tables": {}}}
OUTPUT = {"kind": "data", "id": "result", "version": "1.0.0", "title": "result", "data_kind": "output",
          "storage": {"driver": "stac", "format": "other"}}
DATA = {"nwm-retro": EXTERNAL, "conus-gpkg": MANAGED, "hf-tables": ICEBERG, "result": OUTPUT}


def reader(port_type, data_id, network=False):
    """A tool with one input `src` of `port_type` carrying `data_id`, passed as its only argument."""
    port = {"name": "src", "type": port_type, "data": data_id}
    if port_type in ("file", "directory"):
        port["path"] = "in/src"
    return {"kind": "tool", "id": "reader", "version": "0.1.0", "title": "r", "image": "busybox:1.36",
            "command": ["read"], "args": ["{src}"], "network": network, "inputs": [port],
            "outputs": [{"name": "out", "type": "file", "data": "result", "path": "out.txt"}]}


def registry(port_type, data_id, source=None, network=False, location=None):
    """Data legos, the reader tool, and a one-step pipeline feeding `source` (default: `data_id`) to it;
    `location` replaces the managed data's."""
    source = source or data_id
    data = copy.deepcopy(DATA)
    if location:
        data["conus-gpkg"]["storage"]["location"] = location
    pipeline = {"kind": "pipeline", "id": "p", "version": "0.1.0", "title": "p",
                "steps": [{"name": "read", "tool": {"id": "reader", "version": "0.1.0"},
                           "inputs": {"src": {"data": {"id": source, "version": DATA[source]["version"]}}}}],
                "outputs": {"out": {"step": "read", "output": "out"}}}
    tool = reader(port_type, data_id, network)
    return LEGOS.validate_python(list(data.values()) + [tool, pipeline])


def pipeline_of(legos):
    return next(x for x in legos if isinstance(x, Pipeline))


def step_template(workflow):
    return next(t for t in workflow["spec"]["templates"] if t["name"] == "step-read")


def task(workflow):
    return workflow["spec"]["templates"][0]["dag"]["tasks"][0]


# ---- the models


@pytest.mark.parametrize("location", [
    "conus.gpkg", "managed", "managed/", "managed/../jobs/x", "managed//x", "managed/a b", "managed/{{x}}",
    "/managed/x", "managed/./x",
])
def test_a_managed_location_is_a_plain_key_under_managed(location):
    bad = copy.deepcopy(MANAGED)
    bad["storage"]["location"] = location
    with pytest.raises(ValidationError, match="a key under managed/"):
        LEGOS.validate_python([bad])


def test_a_managed_location_may_be_a_folder():
    folder = copy.deepcopy(MANAGED)
    folder["storage"]["location"] = "managed/conus-gpkg/2.2.0/"
    LEGOS.validate_python([folder])


def test_managed_stac_data_needs_a_location_and_output_data_has_none():
    nowhere = copy.deepcopy(MANAGED)
    del nowhere["storage"]["location"]
    with pytest.raises(ValidationError, match="managed STAC data needs a `location`"):
        LEGOS.validate_python([nowhere])
    somewhere = copy.deepcopy(OUTPUT)
    somewhere["storage"]["location"] = "managed/result/"
    with pytest.raises(ValidationError, match="output data has no `location`"):
        LEGOS.validate_python([somewhere])


@pytest.mark.parametrize("url", [
    "https://example.org/{{workflow.name}}.zarr", "https://example.org/$(HOME).zarr",
    "https://example.org/a b.zarr", "https://example.org/a\n.zarr",
])
def test_an_external_url_holds_nothing_a_command_line_would_expand(url):
    bad = copy.deepcopy(EXTERNAL)
    bad["storage"]["url"] = url
    with pytest.raises(ValidationError, match="may not hold"):
        LEGOS.validate_python([bad])


# ---- the registry checks


@pytest.mark.parametrize("port_type, data_id, network", [
    ("dataset", "nwm-retro", True),
    ("file", "conus-gpkg", False),
    ("dataset", "hf-tables", False),                  # wired right; whether a runtime reads it is render's call
], ids=["external-dataset", "managed-file", "iceberg-dataset"])
def test_a_data_lego_feeds_the_ports_its_driver_serves(port_type, data_id, network):
    assert check(registry(port_type, data_id, network=network)) == []


@pytest.mark.parametrize("port_type, data_id, source, network, message", [
    ("dataset", "nwm-retro", None, False, "external data nwm-retro is read from the internet: "
                                          "reader must declare network: true"),
    ("file", "nwm-retro", None, True, "external data nwm-retro is read in place: it feeds a dataset port, "
                                      "not a file port"),
    ("file", "result", None, False, "result is output data: a run's output is passed as a link or an "
                                    "s3:// input, not by id"),
    ("file", "hf-tables", None, False, "iceberg data hf-tables is read in place: it feeds a dataset port, "
                                       "not a file port"),
    ("file", "conus-gpkg", "result", False, "data lego result doesn't fit a port of conus-gpkg"),
    ("dataset", "conus-gpkg", None, False, "managed data conus-gpkg is staged: it feeds a file or directory "
                                           "port, not a dataset port"),
    ("directory", "conus-gpkg", None, False,
     "managed data conus-gpkg is a file (managed/conus-gpkg/2.2.0/conus_nextgen.gpkg), the port wants a directory"),
], ids=["external-without-network", "external-to-file", "output-by-id", "iceberg-to-file", "other-data",
        "managed-to-dataset", "managed-file-to-directory"])
def test_a_data_lego_on_a_port_its_driver_cant_serve_is_a_problem(port_type, data_id, source, network, message):
    assert check(registry(port_type, data_id, source, network)) == [f"pipeline p@0.1.0: step read.src: {message}"]


# ---- rendering


def test_external_data_gives_the_tool_its_url():
    legos = registry("dataset", "nwm-retro", network=True)
    workflow = render(pipeline_of(legos), legos, {})
    assert step_template(workflow)["container"]["args"] == [URL]
    assert "arguments" not in task(workflow)                 # nothing staged: read in place


@pytest.mark.parametrize("port_type, location", [
    ("file", "managed/conus-gpkg/2.2.0/conus_nextgen.gpkg"),
    ("directory", "managed/conus-gpkg/2.2.0/"),
], ids=["file", "folder"])
def test_managed_data_is_staged_from_its_location(port_type, location):
    legos = registry(port_type, "conus-gpkg", location=location)
    assert check(legos) == []
    workflow = render(pipeline_of(legos), legos, {})
    assert task(workflow)["arguments"]["artifacts"] == [{"name": "src", "s3": {"key": location}}]
    assert step_template(workflow)["container"]["args"] == ["/work/in/src"]


@pytest.mark.parametrize("port_type, data_id, source, network, message", [
    ("dataset", "nwm-retro", None, False, "reader must declare network: true"),
    ("file", "nwm-retro", None, True, "external data nwm-retro is read in place: it feeds a dataset port"),
    ("file", "result", None, False, "result is output data"),
    ("dataset", "conus-gpkg", None, False, "managed data conus-gpkg is staged"),
], ids=["external-without-network", "external-to-file", "output-by-id", "managed-to-dataset"])
def test_render_refuses_what_check_refuses(port_type, data_id, source, network, message):
    legos = registry(port_type, data_id, source, network)       # render doesn't rely on check having run
    with pytest.raises(UnsupportedForArgo, match=r"^step read\.src: .*" + re.escape(message)):
        render(pipeline_of(legos), legos, {})


def test_an_unregistered_data_lego_is_never_rendered():
    legos = [x for x in registry("dataset", "nwm-retro", network=True) if x.id != "nwm-retro"]
    with pytest.raises(UnsupportedForArgo, match="nwm-retro@1.0.0 is not a registered data lego"):
        render(pipeline_of(legos), legos, {})


def test_iceberg_data_has_no_argo_driver_yet():
    legos = registry("dataset", "hf-tables")
    with pytest.raises(UnsupportedForArgo, match="hf-tables is iceberg data: Argo has no driver for it"):
        render(pipeline_of(legos), legos, {})


def test_a_caller_never_gives_a_dataset_url():
    legos = registry("dataset", "nwm-retro", network=True)
    pipeline = pipeline_of(legos).model_copy(update={"inputs": [Port(name="ds", type="dataset", data="nwm-retro")]})
    with pytest.raises(UnsupportedForArgo, match="a dataset comes from a registered data lego"):
        render(pipeline, legos, {"ds": "https://my-lab.org/data.zarr"})


def test_check_reports_an_unregistered_data_lego():
    legos = [x for x in registry("dataset", "nwm-retro", network=True) if x.id != "nwm-retro"]
    assert check(legos) == ["tool reader@0.1.0: port src: unknown data lego 'nwm-retro'",
                            "pipeline p@0.1.0: step read.src: nwm-retro@1.0.0 is not a registered data lego"]


def test_a_scalar_port_cant_take_a_data_lego():
    legos = registry("dataset", "nwm-retro", network=True)
    tool = next(x for x in legos if x.id == "reader")
    tool = tool.model_copy(update={"inputs": [Port(name="src", type="string")]})
    legos = [tool if x.id == "reader" else x for x in legos]
    assert check(legos) == ["pipeline p@0.1.0: step read.src: a string port can't take a data lego"]


def test_an_external_url_reaches_env_as_is():
    legos = registry("dataset", "nwm-retro", network=True)
    tool = next(x for x in legos if x.id == "reader").model_copy(update={"args": [], "env": {"SRC": "{src}"}})
    legos = [tool if x.id == "reader" else x for x in legos]
    container = step_template(render(pipeline_of(legos), legos, {}))["container"]
    assert container["env"] == [{"name": "SRC", "value": URL}]


@pytest.mark.parametrize("url", [
    "https://minio:9000/bucket/x.zarr", "https://argo-server.argo.svc.cluster.local/x",
    "https://metadata.google.internal/x", "https://printer.local/x",
    "https://minio.minio.svc.cluster.local./x", "https://localhost./x", "https://10.0.0.1./x",   # absolute names
])
def test_an_external_https_url_names_a_public_host(url):
    bad = copy.deepcopy(EXTERNAL)
    bad["storage"]["url"] = url
    with pytest.raises(ValidationError, match="is not a public host|is a numeric host|is not a public address"):
        LEGOS.validate_python([bad])


def test_an_s3_bucket_name_is_not_a_host_name():
    one_label = copy.deepcopy(EXTERNAL)
    one_label["storage"]["url"] = "s3://noaa-nwm-retrospective-3-0-pds/CONUS/zarr/chrtout.zarr"
    LEGOS.validate_python([one_label])


@pytest.mark.parametrize("url", ["https://example.org/a\x7f.zarr", "https://example.org/a\x85.zarr",
                                 "https://exa\tmple.org/x"])
def test_control_characters_are_refused_before_the_host_is_read(url):
    bad = copy.deepcopy(EXTERNAL)
    bad["storage"]["url"] = url
    with pytest.raises(ValidationError, match="may not hold"):
        LEGOS.validate_python([bad])


@pytest.mark.parametrize("field, value", [
    ("url", "https://example.org/" + "a" * 2048),
    ("location", "managed/" + "a" * 1024),
])
def test_urls_and_locations_have_a_length_limit(field, value):
    bad = copy.deepcopy(EXTERNAL if field == "url" else MANAGED)
    bad["storage"][field] = value
    with pytest.raises(ValidationError, match="at most"):
        LEGOS.validate_python([bad])


def test_no_workflow_is_named_managed():
    legos = registry("file", "conus-gpkg")
    with pytest.raises(ValueError, match="can't be named 'managed'"):
        render(pipeline_of(legos), legos, {}, name="managed")
