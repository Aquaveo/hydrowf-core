import copy
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from hydrowf_core.models import LEGOS, Pipeline
from hydrowf_core.registry import check

RAW = yaml.safe_load((Path(__file__).parent / "fixtures" / "legos.yaml").read_text())


def registry():
    """A fresh copy of the fixture registry (as dicts) and its pipeline entry."""
    raw = copy.deepcopy(RAW)
    return raw, next(x for x in raw if x["kind"] == "pipeline")


def step(pipeline, name):
    return next(s for s in pipeline["steps"] if s["name"] == name)


def test_fixture_registry_has_no_problems():
    assert check(LEGOS.validate_python(RAW)) == []


def test_order_puts_linked_steps_after_their_sources():
    raw, p = registry()
    p["steps"].reverse()                                  # preprocess listed before subset
    assert Pipeline.model_validate(p).order() == ["subset", "preprocess"]


# ---- wiring the Pipeline model sees on its own

@pytest.mark.parametrize("change, message", [
    (lambda p: p["steps"].append(copy.deepcopy(p["steps"][0])), "step names must be unique"),
    (lambda p: step(p, "subset")["inputs"].update(outlet={"input": "nope"}), "pipeline input 'nope' is not declared"),
    (lambda p: step(p, "preprocess")["inputs"].update(hydrofabric={"step": "nope", "output": "x"}),
     "links to unknown step 'nope'"),
    (lambda p: p["outputs"].update(x={"step": "nope", "output": "y"}), "output x: links to unknown step"),
])
def test_bad_wiring_is_rejected(change, message):
    raw, p = registry()
    change(p)
    with pytest.raises(ValidationError, match=message):
        Pipeline.model_validate(p)


@pytest.mark.parametrize("change", [
    lambda p: step(p, "subset")["inputs"].update(outlet={"step": "preprocess", "output": "run"}),   # a <-> b
    lambda p: step(p, "subset")["inputs"].update(outlet={"step": "subset", "output": "region"}),    # self
])
def test_cycles_are_rejected(change):
    raw, p = registry()
    change(p)
    with pytest.raises(ValidationError, match="cycle"):
        Pipeline.model_validate(p)


# ---- checks that need the other legos (hydrowf_core.registry.check)

@pytest.mark.parametrize("change, message", [
    (lambda p: step(p, "subset").update(tool={"id": "hydrofabric-subset", "version": "9.9.9"}),
     "hydrofabric-subset@9.9.9 is not a registered tool"),
    (lambda p: step(p, "preprocess")["inputs"].pop("start"), "required input 'start' is not connected"),
    (lambda p: step(p, "preprocess")["inputs"].update(extra={"value": 1}), "has no input 'extra'"),
    (lambda p: step(p, "preprocess")["inputs"].update(source={"value": "daymet"}), "not one of"),
    (lambda p: step(p, "preprocess")["inputs"].update(outlet={"value": 5}), "5 is not a string"),
    (lambda p: step(p, "preprocess")["inputs"].update(hydrofabric={"step": "subset", "output": "nope"}),
     "has no output 'nope'"),
    (lambda p: step(p, "preprocess")["inputs"].update(outlet={"step": "subset", "output": "region"}),
     "subset.region is file of hydrofabric-gpkg, the port wants string"),
    (lambda p: step(p, "preprocess")["inputs"].update(
        hydrofabric={"data": {"id": "hydrofabric-v2.2", "version": "1.0.0"}}),
     "fits a dataset port of hydrofabric-v2.2, not a file port of hydrofabric-gpkg"),
    (lambda p: p["outputs"].update(run={"step": "preprocess", "output": "nope"}), "has no output 'nope'"),
])
def test_registry_check_reports_mismatches(change, message):
    raw, p = registry()
    change(p)
    problems = check(LEGOS.validate_python(raw))
    assert len(problems) == 1 and message in problems[0], problems


def test_unknown_data_lego_on_a_tool_port_is_reported():
    raw, _ = registry()
    raw = [x for x in raw if x.get("id") != "ngen-run"]
    assert check(LEGOS.validate_python(raw)) == [
        "tool ngiab-preprocess@4.9.5: port run: unknown data lego 'ngen-run'"]
