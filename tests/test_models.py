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
