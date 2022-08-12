import pyarrow as pa
import pytest

import vast.utils.arrow as vua


def test_schema_name_extraction():
    # Since Arrow cannot attach names to schemas, we do this via metadata.
    schema = pa.schema(
        [("a", "string"), ("b", "string")], metadata={"VAST:name:0": "foo"}
    )
    assert vua.name(schema) == "foo"


def test_schema_alias_extraction():
    # Since Arrow cannot attach names to schemas, we do this via metadata.
    schema = pa.schema(
        [("a", "string"), ("b", "string")],
        metadata={"VAST:name:0": "foo", "VAST:name:1": "bar"},
    )
    names = vua.names(schema)
    assert len(names) == 2
    assert names[0] == "foo"
    assert names[1] == "bar"
    # The first name is the top-level type name.
    assert vua.name(schema) == "foo"
