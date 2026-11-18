import json
import re
from pathlib import Path

import pytest
import yaml

from hydrowf_core.adapters.ogc import S3_URI, UnsupportedForOgc, port_schema, process_description
from hydrowf_core.models import LEGOS, Pipeline
from hydrowf_core.models.lego import Port

FIXTURES = Path(__file__).parent / "fixtures"
LEGO_LIST = LEGOS.validate_python(yaml.safe_load((FIXTURES / "legos.yaml").read_text()))
PIPELINES = {x.id: x for x in LEGO_LIST if isinstance(x, Pipeline)}
S3 = {"type": "string", "format": "uri", "pattern": S3_URI}


def test_simulation_has_the_expected_description():
    expected = json.loads((FIXTURES / "ogc" / "ngiab-simulation.json").read_text())
    assert process_description(PIPELINES["ngiab-simulation"], LEGO_LIST) == expected


@pytest.mark.parametrize("port, schema", [
    ({"name": "x", "type": "string"}, {"type": "string"}),
    ({"name": "x", "type": "string", "default": "a"}, {"type": "string", "default": "a"}),
    ({"name": "x", "type": "string", "pattern": r"gage-\d+|cat-\d+"},
     {"type": "string", "pattern": r"^(?:gage-\d+|cat-\d+)$"}),          # anchored: JSON Schema's aren't
    ({"name": "x", "type": "int", "default": 6}, {"type": "integer", "default": 6}),
    ({"name": "x", "type": "float"}, {"type": "number"}),
    ({"name": "x", "type": "boolean", "default": False}, {"type": "boolean", "default": False}),
    ({"name": "x", "type": "enum", "symbols": ["nwm", "aorc"], "default": "nwm"},
     {"type": "string", "enum": ["nwm", "aorc"], "default": "nwm"}),
    ({"name": "x", "type": "file", "data": "d"}, S3),
    ({"name": "x", "type": "directory", "data": "d"}, S3),
])
def test_port_schema(port, schema):
    assert port_schema(Port.model_validate(port)) == schema


def test_s3_pattern_accepts_any_bucket_and_rejects_other_schemes():
    pattern = re.compile(S3_URI)
    assert pattern.match("s3://my-bucket/hydrofabric/v2.2/conus_nextgen.gpkg")
    assert pattern.match("s3://noaa-nwm-retrospective-3-0-pds/CONUS/zarr/forcing/")
    assert not pattern.match("https://example.org/conus_nextgen.gpkg")
    assert not pattern.match("s3://bucket-only")


def test_dataset_inputs_are_not_supported_yet():
    pipeline = PIPELINES["ngiab-simulation"].model_copy(deep=True)
    pipeline.inputs[3] = Port(name="hydrofabric", type="dataset", data="hydrofabric-v2.2")
    with pytest.raises(UnsupportedForOgc, match="dataset inputs need a data driver"):
        process_description(pipeline, LEGO_LIST)


def test_step_defaults_the_pipeline_does_not_expose_stay_hidden():
    inputs = process_description(PIPELINES["ngiab-simulation"], LEGO_LIST)["inputs"]
    assert "source" not in inputs and "precip_max_gap" not in inputs    # ngiab-preprocess defaults
