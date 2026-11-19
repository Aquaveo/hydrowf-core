from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from hydrowf_core.models import LEGOS, DataLego, ToolLego

FIXTURES = Path(__file__).parent / "fixtures"

TOOL = {"kind": "tool", "id": "t", "version": "0.1.0", "title": "t", "image": "busybox:1.36", "command": ["true"]}
DATA = {"kind": "data", "id": "d", "version": "1.0.0", "title": "d"}


def table(columns, partition=(), sort=()):
    return {**DATA, "data_kind": "managed",
            "storage": {"driver": "iceberg",
                        "tables": {"t": {"columns": columns, "partition": list(partition), "sort": list(sort)}}}}


def test_fixture_legos_validate():
    legos = LEGOS.validate_python(yaml.safe_load((FIXTURES / "legos.yaml").read_text()))
    assert [type(x).__name__ for x in legos].count("DataLego") == 8
    assert [x.id for x in legos if isinstance(x, ToolLego)] == [
        "hydrofabric-subset", "ngiab-preprocess", "ngiab-run", "ngiab-to-cng"]
    assert not isinstance(legos[-1], (DataLego, ToolLego))   # the pipeline


@pytest.mark.parametrize("bad, message", [
    ({**TOOL, "inputs": [{"name": "x", "type": "file"}]}, "needs `data`"),
    ({**TOOL, "inputs": [{"name": "x", "type": "string", "data": "d"}]}, "only file/directory"),
    ({**TOOL, "inputs": [{"name": "x", "type": "enum"}]}, "symbols"),
    ({**TOOL, "image": "busybox"}, "pattern"),                      # unpinned image
    ({**DATA, "data_kind": "external",
      "storage": {"driver": "iceberg", "tables": {}}}, "external"),
    ({**DATA, "data_kind": "external",
      "storage": {"driver": "external", "url": "file:///etc/passwd", "format": "other"}}, "pattern"),
])
def test_invalid_legos_are_rejected(bad, message):
    with pytest.raises(ValidationError, match=message):
        LEGOS.validate_python([bad])


TS = [{"name": "location_id", "type": "string"}, {"name": "value_time", "type": "timestamp"},
      {"name": "day", "type": "date"}, {"name": "geom", "type": "geometry", "crs": 5070}]
NO_CRS = TS[:3] + [{"name": "geom", "type": "geometry"}]


def part(transform, column, width=None):
    return {"transform": transform, "column": column, **({"width": width} if width is not None else {})}


@pytest.mark.parametrize("bad, message", [
    (table(TS, partition=[part("month", "location_id")]), r"month\(location_id\) needs .* is string"),
    (table(TS, partition=[part("hour", "day")]), r"hour\(day\) needs .*timestamp.* is date"),
    (table(TS, partition=[part("identity", "missing")]), "partition column 'missing' is not a column"),
    (table(TS, partition=[part("identity", "geom")]), r"identity\(geom\) needs .* is geometry"),
    (table(TS, partition=[part("bucket", "location_id")]), "needs a positive width"),
    (table(TS, partition=[part("truncate", "location_id", 0)]), "needs a positive width"),
    (table(TS, partition=[part("truncate", "value_time", 4)]), r"truncate\(value_time\) needs"),
    (table(TS, partition=[part("month", "value_time", 3)]), "takes no width"),
    (table(TS, sort=["missing"]), "sort column 'missing' is not a column"),
    (table(NO_CRS), "geometry column 'geom' needs a crs"),
    (table([{"name": "x", "type": "string", "crs": 4326}]), "has a crs but is not a geometry"),
])
def test_bad_table_layouts_are_rejected(bad, message):
    with pytest.raises(ValidationError, match=message):
        LEGOS.validate_python([bad])


@pytest.mark.parametrize("good", [
    table(TS, partition=[part("month", "value_time"), part("identity", "location_id")],
          sort=["location_id", "value_time"]),
    table(TS, partition=[part("day", "day"), part("bucket", "location_id", 16), part("truncate", "location_id", 4)]),
    {**DATA, "data_kind": "managed",
     "storage": {"driver": "iceberg", "tables": {"t": {"columns": {"ref": "teehr.x@1"}, "sort": ["anything"]}}}},
])
def test_good_table_layouts_pass(good):
    LEGOS.validate_python([good])


# ---- bounded fields, and external URLs that can't point inside the cluster


def external(url):
    return {**DATA, "data_kind": "external", "storage": {"driver": "external", "url": url, "format": "other"}}


@pytest.mark.parametrize("bad, message", [
    ({**TOOL, "version": "1.0." + "1" * 40}, "at most 32"),          # services sort versions as numbers
    ({**TOOL, "title": "x" * 201}, "at most 200"),
    ({**TOOL, "description": "x" * 10_001}, "at most 10000"),
    (external("https://" + "a" * 2050 + ".org/"), "at most 2048"),
], ids=["version", "title", "description", "url"])
def test_text_fields_are_bounded(bad, message):
    with pytest.raises(ValidationError, match=message):
        LEGOS.validate_python([bad])


@pytest.mark.parametrize("url", [
    "https://localhost:8080/x", "https://a.localhost/x",
    "https://127.0.0.1/x", "https://10.0.0.5/x", "https://192.168.1.1/x",
    "https://169.254.169.254/latest/meta-data/",                    # cloud metadata
    "https://[::1]/x", "https://[::ffff:10.0.0.1]/x", "https://0.0.0.0/x",
    "https://2130706433/x", "https://0x7f.1/x", "https://127.1/x",  # numeric shorthands for 127.0.0.1
    "s3:///no-bucket",
])
def test_external_urls_must_name_a_public_host(url):
    with pytest.raises(ValidationError, match="public|host"):
        LEGOS.validate_python([external(url)])


@pytest.mark.parametrize("url", [
    "s3://noaa-nwm-retrospective-3-0-pds/CONUS/zarr/forcing/", "https://data.example.org/x.nc",
    "https://8.8.8.8/x",
])
def test_public_external_urls_pass(url):
    assert LEGOS.validate_python([external(url)])[0].storage.url == url


def test_the_limits_are_in_the_json_schema():
    """Forms and agent tools read the schema: they can refuse too-long values before sending them."""
    schema = DataLego.model_json_schema()
    assert schema["properties"]["title"]["maxLength"] == 200
    assert schema["properties"]["version"]["maxLength"] == 32
