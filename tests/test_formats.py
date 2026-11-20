"""Tool-declared output formats: a port lists the data legos it can write, a pipeline step picks one."""
import copy
import json

import pytest
from pydantic import ValidationError

from hydrowf_core.adapters.argo import output_archives, output_formats, render
from hydrowf_core.adapters.ogc import process_description
from hydrowf_core.models import LEGOS, Pipeline
from hydrowf_core.registry import check


def data(id, archive="none", version="1.0.0", title=None):
    return {"kind": "data", "id": id, "version": version, "title": title or id, "data_kind": "output",
            "storage": {"driver": "stac", "format": "other", "archive": archive}}


def tool(id, **extra):
    return {"kind": "tool", "id": id, "version": "0.1.0", "title": id, "image": "busybox:1.36",
            "command": [id], **extra}


TWO = ["cng", "teehr"]                    # the two formats writer can write
VERBOSE = {"name": "verbose", "type": "boolean", "default": False}

# model-run writes its native format only; writer can write two (cng and teehr); evaluate reads teehr
WRITER = tool("writer", args=["{run_dir}", "--out", "{workdir}/out", "--format", "{out_format}"],
              inputs=[{"name": "run_dir", "type": "directory", "data": "native"}],
              outputs=[{"name": "out", "type": "directory", "formats": TWO, "path": "out"}])
BASE = [
    data("native", archive="tgz"), data("cng", archive="tgz", title="Cloud-native files"),
    data("teehr", title="TEEHR time series"),
    tool("model-run", outputs=[{"name": "run_dir", "type": "directory", "data": "native", "path": "run"}]),
    WRITER,
    tool("evaluate", args=["{series}"], inputs=[{"name": "series", "type": "directory", "data": "teehr"}],
         outputs=[{"name": "metrics", "type": "file", "data": "cng", "path": "metrics.parquet"}]),
]
PIPELINE = {
    "kind": "pipeline", "id": "simulate-evaluate", "version": "0.1.0", "title": "p",
    "steps": [
        {"name": "run", "tool": {"id": "model-run", "version": "0.1.0"}},
        {"name": "write", "tool": {"id": "writer", "version": "0.1.0"},
         "inputs": {"run_dir": {"step": "run", "output": "run_dir"}}, "formats": {"out": "teehr"}},
        {"name": "eval", "tool": {"id": "evaluate", "version": "0.1.0"},
         "inputs": {"series": {"step": "write", "output": "out"}}},
    ],
    "outputs": {"series": {"step": "write", "output": "out"}, "metrics": {"step": "eval", "output": "metrics"}},
}


def legos(pipeline=None, extra=()):
    return LEGOS.validate_python(copy.deepcopy(BASE) + list(extra) + [copy.deepcopy(pipeline or PIPELINE)])


def pipeline_of(found):
    return next(x for x in found if isinstance(x, Pipeline))


def without_choice():
    p = copy.deepcopy(PIPELINE)
    del p["steps"][1]["formats"]
    return p


def template(workflow, step):
    return next(t for t in workflow["spec"]["templates"] if t["name"] == f"step-{step}")


# ---- the models


def output(name="out", **extra):
    return {"name": name, "type": "directory", "path": name, **extra}


@pytest.mark.parametrize("bad, message", [
    (tool("t", outputs=[output(formats=["cng"])], args=["{out_format}"]), "at least two different"),
    (tool("t", outputs=[output(formats=["cng", "cng"])], args=["{out_format}"]), "at least two different"),
    (tool("t", outputs=[output(data="cng", formats=TWO)], args=["{out_format}"]),
     "`formats` replaces `data`"),
    (tool("t", outputs=[{"name": "n", "type": "string", "formats": TWO}]),
     "only file/directory ports list `formats`"),
    (tool("t", outputs=[{"name": "d", "type": "dataset", "formats": TWO}]),
     "only file/directory ports list `formats`"),
    (tool("t", inputs=[{"name": "i", "type": "directory", "formats": TWO}]),
     "only outputs list `formats`"),
    (tool("t", outputs=[output(formats=TWO)]),
     r"pass \{out_format\} in args \(not only in a group\) or env"),
    (tool("t", outputs=[output(formats=TWO), output("b", formats=TWO)], args=["{out_format}"]),
     r"pass \{b_format\} in args"),
    (tool("t", outputs=[output(formats=TWO)], inputs=[VERBOSE],
          args=[{"when": "verbose", "args": ["--format", "{out_format}"]}]),           # dropped when verbose is false
     r"pass \{out_format\} in args \(not only in a group\)"),
    (tool("t", outputs=[output(formats=TWO)], args=["{out_format}"],
          inputs=[{"name": "i", "type": "directory", "data": "cng", "path": "in/{out_format}"}]),
     r"i path .*\{out_format\} is chosen by the pipeline"),
    (tool("t", outputs=[output(formats=TWO)], args=["{out_format}"], files={"{out_format}/x": ""}),
     r"files .*\{out_format\} is chosen by the pipeline"),
    (tool("t", outputs=[output(formats=TWO)], args=["{out_format}", {"when": "out_format", "args": ["-x"]}]),
     "when 'out_format' is not an input"),
    (tool("t", outputs=[output(formats=TWO)], args=["{out_format}"],
          inputs=[{"name": "out_format", "type": "string"}]), "reserved for the format of output 'out'"),
    (tool("t", outputs=[output(formats=TWO, path="{out_format}")], args=["{out_format}"]),
     r"\{out_format\} is chosen by the pipeline; use it in args or env, not in paths"),
    (tool("t", outputs=[output()]), r"a directory port needs `data` \(a data lego id; for outputs, or `formats`\)"),
])
def test_invalid_format_declarations_are_rejected(bad, message):
    with pytest.raises(ValidationError, match=message):
        LEGOS.validate_python([bad])


def test_the_format_placeholder_may_be_in_env_and_also_in_a_group():
    LEGOS.validate_python([
        tool("t", outputs=[output(formats=TWO)], env={"FORMAT": "{out_format}"}),
        tool("u", outputs=[output(formats=TWO)], env={"F": "{out_format}"},   # always told
             inputs=[VERBOSE],
             args=[{"when": "verbose", "args": ["--log-format", "{out_format}"]}]),
    ])


def test_a_pipeline_input_cannot_list_formats():
    p = {**copy.deepcopy(PIPELINE), "inputs": [{"name": "i", "type": "directory", "formats": TWO}]}
    with pytest.raises(ValidationError, match="pipeline input i: only tool outputs list `formats`"):
        Pipeline.model_validate(p)


# ---- the registry checks


def test_a_step_that_chose_teehr_feeds_a_port_that_wants_teehr():
    assert check(legos()) == []


def test_without_a_choice_the_first_format_is_written_and_wiring_follows_it():
    problems = check(legos(without_choice()))
    assert problems == ["pipeline simulate-evaluate@0.1.0: step eval.series: write.out is directory of cng, "
                        "the port wants directory of teehr"]


@pytest.mark.parametrize("formats, message, written", [
    ({"nope": "teehr"}, "formats: writer has no output 'nope'", "cng"),                  # out: the first
    ({"out": "native"}, "formats: output 'out' of writer writes cng, teehr, not 'native'", "native"),
], ids=["unknown-output", "not-listed"])
def test_a_step_can_only_choose_a_listed_format(formats, message, written):
    p = copy.deepcopy(PIPELINE)
    p["steps"][1]["formats"] = formats
    wiring = (f"pipeline simulate-evaluate@0.1.0: step eval.series: write.out is directory of {written}, "
              "the port wants directory of teehr")
    assert check(legos(p)) == [f"pipeline simulate-evaluate@0.1.0: step write: {message}", wiring]


def test_a_one_format_output_offers_no_choice():
    p = copy.deepcopy(PIPELINE)
    p["steps"][0]["formats"] = {"run_dir": "native"}
    assert check(legos(p)) == ["pipeline simulate-evaluate@0.1.0: step run: formats: output 'run_dir' of "
                               "model-run has one format, 'native'"]


def test_every_format_must_be_a_registered_data_lego():
    found = legos(extra=[tool("t", outputs=[output(formats=["cng", "ghost"])], args=["{out_format}"])])
    assert check(found) == ["tool t@0.1.0: port out: unknown data lego 'ghost'"]


# ---- rendering


def test_the_tool_is_told_the_chosen_format():
    found = legos()
    workflow = render(pipeline_of(found), found, {})
    assert template(workflow, "write")["container"]["args"] == ["/work/inputs/run_dir", "--out", "/work/out",
                                                                "--format", "teehr"]


def test_without_a_choice_the_tool_is_told_the_first_format():
    found = legos(without_choice())
    workflow = render(pipeline_of(found), found, {})
    assert template(workflow, "write")["container"]["args"][-1] == "cng"


def test_the_chosen_formats_storage_decides_the_archive():
    found = legos()                                   # teehr: files; cng (the first format) would be tgz
    out = template(render(pipeline_of(found), found, {}), "write")["outputs"]["artifacts"][0]
    assert out["archive"] == {"none": {}} and out["s3"]["key"].endswith("/out/")
    found = legos(without_choice())                   # cng: Argo's default .tgz
    out = template(render(pipeline_of(found), found, {}), "write")["outputs"]["artifacts"][0]
    assert "archive" not in out


def test_the_workflow_records_each_outputs_data_lego_and_its_latest_version():
    found = legos(extra=[data("teehr", version="1.10.0"), data("teehr", version="1.9.0")])
    annotation = render(pipeline_of(found), found, {})["metadata"]["annotations"]["hydrowf/formats"]
    assert json.loads(annotation) == {"series": "teehr@1.10.0", "metrics": "cng@1.0.0"}
    assert output_formats(pipeline_of(found), found) == json.loads(annotation)


def test_the_process_description_names_the_chosen_format():
    found = legos()
    outputs = process_description(pipeline_of(found), found)["outputs"]
    assert outputs["series"]["description"] == "(TEEHR time series)"
    assert outputs["metrics"]["description"] == "(Cloud-native files)"


def test_the_env_gets_the_chosen_format_too():
    env_writer = {**copy.deepcopy(WRITER), "id": "env-writer", "args": ["{run_dir}"], "env": {"FORMAT": "{out_format}"}}
    p = copy.deepcopy(PIPELINE)
    p["steps"][1]["tool"]["id"] = "env-writer"
    found = legos(p, extra=[env_writer])
    assert template(render(pipeline_of(found), found, {}), "write")["container"]["env"] == [
        {"name": "FORMAT", "value": "teehr"}]


def test_a_multi_format_file_output_is_stored_as_its_chosen_format():
    file_writer = {**copy.deepcopy(WRITER), "id": "file-writer",
                   "outputs": [{"name": "out", "type": "file", "formats": TWO, "path": "out/series.parquet"}]}
    p = copy.deepcopy(PIPELINE)
    p["steps"][1]["tool"]["id"] = "file-writer"
    del p["steps"][2]                                       # evaluate wants a directory
    del p["outputs"]["metrics"]
    found = legos(p, extra=[file_writer])
    out = template(render(pipeline_of(found), found, {}), "write")["outputs"]["artifacts"][0]
    assert out["archive"] == {"none": {}}                   # teehr's storage (cng's would be tgz)
    assert out["s3"]["key"] == "{{workflow.name}}/{{pod.name}}/out/series.parquet"


def test_an_output_whose_tool_or_data_is_unknown_is_left_out_of_the_annotation():
    found = legos()
    no_teehr = [x for x in found if x.id != "teehr"]
    assert output_formats(pipeline_of(found), no_teehr) == {"metrics": "cng@1.0.0"}
    no_evaluate = [x for x in found if x.id != "evaluate"]
    assert output_formats(pipeline_of(found), no_evaluate) == {"series": "teehr@1.0.0"}


def test_the_annotation_and_the_archive_come_from_the_same_version():
    newest = {"kind": "data", "id": "teehr", "version": "2.0.0", "title": "t", "data_kind": "managed",
              "storage": {"driver": "iceberg", "tables": {}}}
    found = legos(extra=[newest])
    assert output_formats(pipeline_of(found), found)["series"] == "teehr@2.0.0"
    assert "teehr" not in output_archives(found)            # its latest isn't STAC: stored as files
