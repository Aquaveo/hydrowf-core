from pathlib import Path

import pytest
import yaml

from hydrowf_core.adapters.argo import BadInputs, UnsupportedForArgo, output_archives, render, resources
from hydrowf_core.models import LEGOS, DataLego, Pipeline, ToolLego

FIXTURES = Path(__file__).parent / "fixtures"
LEGO_LIST = LEGOS.validate_python(yaml.safe_load((FIXTURES / "legos.yaml").read_text()))
PIPELINES = {x.id: x for x in LEGO_LIST if isinstance(x, Pipeline)}
NGEN = "awiciroh/ciroh-ngen-image@sha256:d5ee1a6c15c025f9be2f9ce140dd9cdc2e933bc30f531a41f180d45b15c8ab17"
ENTRYPOINTS = {NGEN: ["/ngen/HelloNGEN.sh"]}

INPUTS = {"feature": "gage-01031510", "start_date": "2017-10-01", "end_date": "2019-09-30",
          "hydrofabric": "s3://hydrowf/inputs/hydrofabric/v2.2/conus_nextgen.gpkg"}


def simulation(inputs=INPUTS, entrypoint=ENTRYPOINTS.get):
    return render(PIPELINES["ngiab-simulation"], LEGO_LIST, inputs, entrypoint)


def template(workflow, name):
    return next(t for t in workflow["spec"]["templates"] if t["name"] == name)


def test_simulation_renders_the_expected_workflow():
    assert simulation() == yaml.safe_load((FIXTURES / "argo" / "ngiab-simulation.yaml").read_text())


def test_keep_days_has_argo_delete_the_finished_run_and_so_its_outputs():
    spec = render(PIPELINES["ngiab-simulation"], LEGO_LIST, INPUTS, ENTRYPOINTS.get, keep_days=30)["spec"]
    assert spec["ttlStrategy"] == {"secondsAfterCompletion": 30 * 86400}
    assert spec["artifactGC"] == {"strategy": "OnWorkflowDeletion"}


def test_without_keep_days_a_run_stays_until_deleted():
    assert "ttlStrategy" not in simulation()["spec"]


@pytest.mark.parametrize("keep_days", [0, -1, 1.5, True, "30", 3651])
def test_keep_days_is_a_whole_number_of_days(keep_days):
    with pytest.raises(ValueError, match="keep_days"):
        render(PIPELINES["ngiab-simulation"], LEGO_LIST, INPUTS, ENTRYPOINTS.get, keep_days=keep_days)


@pytest.mark.parametrize("cpu, ram_mb, cores", [(4, 16384, "4"), (0.5, 512, "0.5"), (1, 1024, "1")])
def test_memory_is_capped_at_the_request_and_cpu_is_not(cpu, ram_mb, cores):
    tool = ToolLego(kind="tool", id="t", version="0.1.0", title="t", image="busybox:1.36", cpu=cpu, ram_mb=ram_mb)
    assert resources(tool) == {"requests": {"cpu": cores, "memory": f"{ram_mb}Mi"},
                               "limits": {"memory": f"{ram_mb}Mi"}}


def test_inputs_reach_the_command_line_and_artifacts():
    wf = simulation({**INPUTS, "feature": "cat-5173"})
    args = template(wf, "step-preprocess")["container"]["args"]
    assert args[args.index("-i") + 1] == "cat-5173"
    task = wf["spec"]["templates"][0]["dag"]["tasks"][0]
    assert task["arguments"]["artifacts"] == [{"name": "hydrofabric",
                                               "s3": {"key": "inputs/hydrofabric/v2.2/conus_nextgen.gpkg"}}]


def test_run_becomes_the_workflow_name():
    args = template(simulation(), "step-to-cng")["container"]["args"]
    assert args[args.index("--run-id") + 1] == "{{workflow.name}}"


def test_a_given_name_replaces_the_generated_one():
    job = "1097b650-e1f9-11f1-8c4c-0c54150d835b"          # a pygeoapi job id (uuid1)
    wf = render(PIPELINES["ngiab-simulation"], LEGO_LIST, INPUTS, ENTRYPOINTS.get, name=job)
    assert wf["metadata"]["name"] == job and "generateName" not in wf["metadata"]
    assert simulation()["metadata"]["generateName"] == "ngiab-simulation-"


def test_without_an_entrypoint_argo_looks_it_up():
    run = template(simulation(entrypoint=None), "step-run")["container"]
    assert "command" not in run and run["args"][1:] == ["auto", "4", "local"]


def test_iceberg_data_has_no_argo_driver():
    with pytest.raises(UnsupportedForArgo, match="hydrofabric-v2.2 is iceberg data: Argo has no driver for it"):
        render(PIPELINES["ngiab-prepare"], LEGO_LIST, {"outlet": "gage-1", "start": "a", "end": "b"})


@pytest.mark.parametrize("inputs, message", [
    ({**INPUTS, "nope": 1}, r"has no inputs \['nope'\]"),
    ({k: v for k, v in INPUTS.items() if k != "start_date"}, "input start_date is required"),
    ({**INPUTS, "feature": 5}, "input feature: 5 is not a string"),
    ({**INPUTS, "hydrofabric": "/data/conus_nextgen.gpkg"}, "is an s3://<bucket>/<key> URL"),
])
def test_bad_inputs_are_rejected_before_anything_is_rendered(inputs, message):
    with pytest.raises(BadInputs, match=message):
        simulation(inputs)


@pytest.mark.parametrize("name, value", [
    ("feature", "gage-{{=sprig.repeat(2, '.')}}"),     # an Argo expression: evaluated in the controller
    ("feature", "gage-{{workflow.name}}"),             # an Argo variable
    ("feature", "gage-{"),                             # half of one: it could join registry text after it
    ("configuration_name", "x}"),
    ("configuration_name", "$(HOME)"),                 # Kubernetes expands $(VAR) in args and env
    ("configuration_name", "cfe$"),                    # half of one: "(HOME)" after it would complete it
    ("configuration_name", "cfe\nx"),                  # a control character
    ("hydrofabric", "s3://my-bucket/{{workflow.name}}/x.gpkg"),   # artifact keys are expanded too
])
def test_caller_text_with_template_syntax_is_rejected(name, value):
    with pytest.raises(BadInputs, match=rf"input {name}: \{{, \}}, \$ and control characters are not allowed"):
        simulation({**INPUTS, name: value})


def test_caller_text_has_a_length_limit():
    with pytest.raises(BadInputs, match="input configuration_name: longer than 1024 characters"):
        simulation({**INPUTS, "configuration_name": "x" * 1025})


def test_defaults_are_the_registrys_text_not_the_callers():
    pipeline = PIPELINES["ngiab-simulation"]
    inputs = [p.model_copy(update={"default": "{{workflow.name}}"}) if p.name == "configuration_name" else p
              for p in pipeline.inputs]
    workflow = render(pipeline.model_copy(update={"inputs": inputs}), LEGO_LIST, INPUTS, ENTRYPOINTS.get)
    assert "{{workflow.name}}" in template(workflow, "step-to-cng")["container"]["args"]


def with_pattern(name, pattern):
    """ngiab-simulation with a pattern on one of its inputs."""
    pipeline = PIPELINES["ngiab-simulation"]
    inputs = [p.model_copy(update={"pattern": pattern}) if p.name == name else p for p in pipeline.inputs]
    return pipeline.model_copy(update={"inputs": inputs})


def test_a_tool_ports_pattern_holds_for_values_from_the_pipeline():
    legos = [x.model_copy(update={"inputs": [p.model_copy(update={"pattern": "gage-[0-9]+"})
                                             if p.name == "feature" else p for p in x.inputs]})
             if x.id == "ngiab-preprocess" else x for x in LEGO_LIST]
    render(PIPELINES["ngiab-simulation"], legos, INPUTS, ENTRYPOINTS.get)   # gage-01031510
    with pytest.raises(BadInputs, match=r"input feature \(step preprocess.feature\): 'cat-5173' does not match"):
        render(PIPELINES["ngiab-simulation"], legos, {**INPUTS, "feature": "cat-5173"}, ENTRYPOINTS.get)


def tiny():
    """A tool whose output path is an input, and a pipeline that hands that input to the caller."""
    return LEGOS.validate_python([
        {"kind": "tool", "id": "t", "version": "0.1.0", "title": "t", "image": "busybox:1.36",
         "args": ["{name}"], "inputs": [{"name": "name", "type": "string"}],
         "outputs": [{"name": "out", "type": "directory", "data": "d", "path": "{name}"}]},
        {"kind": "pipeline", "id": "p", "version": "0.1.0", "title": "p",
         "inputs": [{"name": "name", "type": "string"}],
         "steps": [{"name": "s", "tool": {"id": "t", "version": "0.1.0"}, "inputs": {"name": {"input": "name"}}}],
         "outputs": {"out": {"step": "s", "output": "out"}}},
    ])


@pytest.mark.parametrize("value", ["../../var/run/secrets/kubernetes.io/serviceaccount", "a/../../x", "/etc",
                                   "a//b", "", "."])
def test_a_path_an_input_fills_stays_under_the_workdir(value):
    legos = tiny()
    with pytest.raises(BadInputs, match="outside /work"):
        render(legos[-1], legos, {"name": value})


def test_a_path_an_input_fills_may_have_folders():
    legos = tiny()
    workflow = render(legos[-1], legos, {"name": "runs/ngen-run"})
    assert template(workflow, "step-s")["outputs"]["artifacts"][0]["path"] == "/work/runs/ngen-run"


def test_an_input_must_match_its_ports_pattern():
    pipeline = with_pattern("start_date", r"\d{4}-\d{2}-\d{2}( \d{2}:\d{2})?")
    render(pipeline, LEGO_LIST, INPUTS, ENTRYPOINTS.get)                     # 2017-10-01 matches
    render(pipeline, LEGO_LIST, {**INPUTS, "start_date": "2017-10-01 06:00"}, ENTRYPOINTS.get)
    for bad in ("2017/10/01", "2017-10-01T06:00", "2017-10-01 06:00 junk",   # the whole value must match
                "\u0662\u0660\u0661\u0667-\u0661\u0660-\u0660\u0661"):     # Arabic-Indic digits: \d is ASCII
        with pytest.raises(BadInputs, match="input start_date: .* does not match"):
            render(pipeline, LEGO_LIST, {**INPUTS, "start_date": bad}, ENTRYPOINTS.get)


def test_caller_labels_join_renders_own():
    workflow = render(PIPELINES["ngiab-simulation"], LEGO_LIST, INPUTS, ENTRYPOINTS.get,
                      labels={"hydrowf/owner": "user-1"})
    assert workflow["metadata"]["labels"] == {"hydrowf/owner": "user-1", "hydrowf/pipeline": "ngiab-simulation",
                                              "hydrowf/version": "0.1.0"}


@pytest.mark.parametrize("labels, message", [
    ({"hydrowf/pipeline": "other"}, "render\\(\\) sets these labels itself"),
    ({"hydrowf/version": "9.9.9"}, "render\\(\\) sets these labels itself"),
    ({"hydrowf/owner": "a,b"}, "not a valid Kubernetes label"),
    ({"hydrowf/owner": "x" * 64}, "not a valid Kubernetes label"),
    ({"bad key!": "x"}, "not a valid Kubernetes label"),
    ({"-./x": "x"}, "not a valid Kubernetes label"),             # the prefix is a DNS subdomain
    ({("a" * 64) + "/x": "x"}, "not a valid Kubernetes label"),
    ({1: "x"}, "not a valid Kubernetes label"),                   # a ValueError, never a TypeError
])
def test_caller_labels_are_checked(labels, message):
    with pytest.raises(ValueError, match=message):
        render(PIPELINES["ngiab-simulation"], LEGO_LIST, INPUTS, ENTRYPOINTS.get, labels=labels)


# ---- how outputs are stored: their data lego's archive (none: files; tgz: one .tgz)


def outputs_of(workflow):
    """step template -> {output artifact name: its archive setting, or None for Argo's default (.tgz)}"""
    return {t["name"]: {a["name"]: a.get("archive") for a in t["outputs"]["artifacts"]}
            for t in workflow["spec"]["templates"] if "container" in t}


def output_keys(workflow):
    """step template -> {output artifact name: its explicit S3 key, or None for the repository default}"""
    return {t["name"]: {a["name"]: (a.get("s3") or {}).get("key") for a in t["outputs"]["artifacts"]}
            for t in workflow["spec"]["templates"] if "container" in t}


def with_archive(data_id, archive, version=None):
    """The fixture legos with `data_id`'s storage set to `archive` (as a new version when given)."""
    changed = []
    for lego in LEGO_LIST:
        if isinstance(lego, DataLego) and lego.id == data_id:
            storage = lego.storage.model_copy(update={"archive": archive})
            new = lego.model_copy(update={"storage": storage, **({"version": version} if version else {})})
            changed += [lego, new] if version else [new]
        else:
            changed.append(lego)
    return changed


def test_outputs_are_files_unless_their_data_says_tgz():
    outputs = outputs_of(simulation())
    assert outputs["step-run"] == {"ngen-run": None}                       # ngen-run: archive: tgz
    assert outputs["step-to-cng"] == {"cng": {"none": {}}}                 # ngen-cng: the default, files


def test_a_data_lego_without_archive_means_files():
    legos = with_archive("ngen-run", "none")
    workflow = render(PIPELINES["ngiab-simulation"], legos, INPUTS, ENTRYPOINTS.get)
    assert outputs_of(workflow)["step-run"] == {"ngen-run": {"none": {}}}


def test_the_latest_version_of_a_data_lego_decides():
    nine = with_archive("ngen-cng", "none", version="9.0.0")
    ten = with_archive("ngen-cng", "tgz", version="10.0.0")
    newer = [x for x in ten if x.id == "ngen-cng" and x.version == "10.0.0"]
    older = [x for x in nine if not (x.id == "ngen-cng" and x.version == "10.0.0")]
    legos = newer + older                                   # the newest first: order doesn't decide
    assert output_archives(legos)["ngen-cng"] == "tgz"      # 10.0.0 > 9.0.0 as numbers (not as text)
    workflow = render(PIPELINES["ngiab-simulation"], legos, INPUTS, ENTRYPOINTS.get)
    assert outputs_of(workflow)["step-to-cng"] == {"cng": None}


def test_archive_is_none_or_tgz():
    with pytest.raises(ValueError, match="none|tgz"):
        LEGOS.validate_python([{"kind": "data", "id": "d", "version": "1.0.0", "title": "d", "data_kind": "output",
                                "storage": {"driver": "stac", "format": "other", "archive": "zip"}}])


def test_a_directory_output_key_ends_in_a_slash_so_gc_deletes_its_files():
    keys = output_keys(simulation())
    assert keys["step-to-cng"] == {"cng": "{{workflow.name}}/{{pod.name}}/cng/"}
    assert keys["step-run"] == {"ngen-run": None}                          # tgz: the repository default


def test_a_file_output_keeps_its_name_in_its_own_folder():
    from hydrowf_core.adapters.argo import output_artifact
    from hydrowf_core.models import Port
    port = Port(name="region", type="file", data="hydrofabric-gpkg", path="out/region.gpkg")
    artifact = output_artifact(port, "/work/out/region.gpkg", {})
    assert artifact["s3"] == {"key": "{{workflow.name}}/{{pod.name}}/region/region.gpkg"}
    assert artifact["archive"] == {"none": {}}
