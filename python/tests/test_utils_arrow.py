import ipaddress
import pyarrow as pa
import pytest

import vast.utils.arrow as vua


def test_unpack_ip():
    bytes = b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\n\x01\x15\xa5"
    assert vua.unpack_ip(bytes) == ipaddress.IPv4Address("10.1.21.165")


def test_ip_address_extension_type():
    ty = vua.IPAddressType()
    bytes = b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\n\x01\x15\xa5"
    storage = pa.array([bytes], pa.binary(16))
    arr = pa.ExtensionArray.from_storage(ty, storage)
    arr.validate()
    assert arr.type is ty
    assert arr.storage.equals(storage)
    assert arr[0].as_py() == ipaddress.IPv4Address("10.1.21.165")


def test_subnet_extension_type():
    ty = vua.SubnetType()
    bytes = b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\n\x01\x15\x00\x18"
    storage = pa.array([bytes], pa.binary(17))
    arr = pa.ExtensionArray.from_storage(ty, storage)
    arr.validate()
    assert arr.type is ty
    assert arr.storage.equals(storage)
    assert arr[0].as_py() == ipaddress.IPv4Network("10.1.21.0/24")


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
