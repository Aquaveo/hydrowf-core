from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from hydrowf_core.models import LEGOS, ToolLego
from hydrowf_core.models.lego import fill

FIXTURE = LEGOS.validate_python(yaml.safe_load((Path(__file__).parent / "fixtures" / "legos.yaml").read_text()))
TOOLS = {x.id: x for x in FIXTURE if isinstance(x, ToolLego)}

TOOL = {"kind": "tool", "id": "t", "version": "0.1.0", "title": "t", "image": "busybox:1.36"}


def port(name, type_="string", **extra):
    return {"name": name, "type": type_, **extra}


def values(tool, **given):
    """What a runtime passes to argv: defaults, then the given inputs, then workdir and cpu."""
    return {**{p.name: p.default for p in tool.inputs}, "workdir": tool.workdir, "cpu": tool.cpu, **given}


@pytest.mark.parametrize("bad, message", [
    ({**TOOL, "args": ["{nope}"]}, r"\{nope\} is not an input"),
    ({**TOOL, "env": {"A": "{nope}"}}, r"env A .*\{nope\} is not an input"),
    ({**TOOL, "inputs": [port("m", optional=True)], "args": ["{m}"]}, "is optional; put it in a group with when: m"),
    ({**TOOL, "args": [{"when": "nope", "args": ["-x"]}]}, "when 'nope' is not an input"),
    ({**TOOL, "command": ["run", "{x}"]}, "placeholders go in `args`"),
    ({**TOOL, "inputs": [port("x"), port("x")]}, "input names must be unique"),
    ({**TOOL, "inputs": [port("cpu", "int")]}, "reserved for the runtime"),
    ({**TOOL, "outputs": [port("o", "directory", data="d")]}, "a directory output needs a `path`"),
    ({**TOOL, "inputs": [port("s", path="x")]}, "only file/directory ports have a `path`"),
    ({**TOOL, "inputs": [port("f", "file", data="d", path="/etc/passwd")]}, "must be a relative path"),
    ({**TOOL, "files": {"a/../../x": ""}}, "'..' is not allowed"),
    ({**TOOL, "workdir": "work"}, "must be an absolute path"),
    ({**TOOL, "inputs": [port("f", "file", data="d", default="x")]}, "file port can't take a constant"),
    ({**TOOL, "inputs": [port("n", "int", default="6")]}, "'6' is not a int"),
    ({**TOOL, "inputs": [port("s", "enum", symbols=["a"], default="b")]}, "'b' is not one of"),
])
def test_invalid_tools_are_rejected(bad, message):
    with pytest.raises(ValidationError, match=message):
        LEGOS.validate_python([bad])


@pytest.mark.parametrize("good", [
    {**TOOL, "inputs": [port("n", "int", optional=True, default=1)], "args": ["{n}"]},     # default: always set
    {**TOOL, "inputs": [port("m", optional=True)], "args": [{"when": "m", "args": ["--m", "{m}"]}]},
    {**TOOL, "args": ["{workdir}/out", "{cpu}"]},                                           # no command: ENTRYPOINT
])
def test_valid_tools_pass(good):
    LEGOS.validate_python([good])


def test_fill_formats_like_a_command_line():
    assert fill("{a} {b} {c} {d}", {"a": True, "b": 4.0, "c": 0.5, "d": "x"}) == "true 4 0.5 x"


DATES = {"feature": "gage-01646500", "start_date": "2020-01-01", "end_date": "2020-01-31"}


def test_argv_drops_groups_whose_input_is_unset():
    tool = TOOLS["ngiab-preprocess"]
    assert tool.argv(values(tool, **DATES)) == [
        "--output_root", "/work", "-o", "/work/ngen-run", "-s", "-f", "-r",
        "-i", "gage-01646500", "--start_date", "2020-01-01", "--end_date", "2020-01-31",
        "--source", "nwm", "--subset_type", "nexus", "--precip_max_gap", "6"]


def test_argv_keeps_groups_whose_input_is_set():
    tool = TOOLS["ngiab-preprocess"]
    argv = tool.argv(values(tool, **DATES, model="lstm", nwm_gw=True))
    assert argv[argv.index("2020-01-31") + 1] == "--lstm"
    assert argv[argv.index("nexus") + 1] == "--nwm_gw"


def test_argv_fills_runtime_values():
    tool = TOOLS["ngiab-run"]
    assert tool.argv(values(tool, ngen_run="/work/ngen-run")) == ["/work/ngen-run", "auto", "4", "local"]
