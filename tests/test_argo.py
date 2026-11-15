from pathlib import Path

import pytest
import yaml

from hydrowf_core.adapters.argo import BadInputs, UnsupportedForArgo, render
from hydrowf_core.models import LEGOS, Pipeline

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


def test_inputs_reach_the_command_line_and_artifacts():
    wf = simulation({**INPUTS, "feature": "cat-5173"})
    args = template(wf, "step-preprocess")["container"]["args"]
    assert args[args.index("-i") + 1] == "cat-5173"
    task = wf["spec"]["templates"][0]["dag"]["tasks"][0]
    assert task["arguments"]["artifacts"] == [{"name": "hydrofabric",
                                               "s3": {"key": "inputs/hydrofabric/v2.2/conus_nextgen.gpkg"}}]


def test_without_an_entrypoint_argo_looks_it_up():
    run = template(simulation(entrypoint=None), "step-run")["container"]
    assert "command" not in run and run["args"][1:] == ["auto", "4", "local"]


def test_dataset_inputs_need_a_data_driver():
    with pytest.raises(UnsupportedForArgo, match="dataset inputs need a data driver"):
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
