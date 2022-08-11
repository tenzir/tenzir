# FIXME: this is just a temporary hack to tests to work; we need to adjust the
# package structure.
import sys
pkg_dir = "."
sys.path.append(pkg_dir)

import pyarrow as pa
import pytest

import utils.arrow

def test_schema_name_extraction():
    # Since Arrow cannot attach names to schemas, we do this via metadata.
    schema = pa.schema(
            [("a", "string"), ("b", "string")],
            metadata={"VAST:name:0": "foo"})
    assert utils.arrow.name(schema) == "foo"

def test_schema_alias_extraction():
    # Since Arrow cannot attach names to schemas, we do this via metadata.
    schema = pa.schema(
            [("a", "string"), ("b", "string")],
            metadata={"VAST:name:0": "foo", "VAST:name:1": "bar"})
    names = utils.arrow.names(schema)
    assert len(names) == 2
    assert names[0] == "foo"
    assert names[1] == "bar"
    # The first name is the top-level type name.
    assert utils.arrow.name(schema) == "foo"
