import ipaddress
import pyarrow as pa
import pytest

import vast.utils.arrow as vua


def test_unpack_ip():
    bytes = b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\n\x01\x15\xa5"
    assert vua.unpack_ip(bytes) == ipaddress.IPv4Address("10.1.21.165")


def test_ip_address_extension_type():
    ty = vua.AddressType()
    bytes = b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\n\x01\x15\xa5"
    storage = pa.array([bytes], pa.binary(16))
    arr = pa.ExtensionArray.from_storage(ty, storage)
    arr.validate()
    assert arr.type is ty
    assert arr.storage.equals(storage)
    assert arr[0].as_py() == ipaddress.IPv4Address("10.1.21.165")


def test_subnet_extension_type():
    ty = vua.SubnetType()
    bytes = b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\n\x01\x15\x00"
    storage = pa.StructArray.from_arrays(
        [
            # pa.array([bytes, bytes], type=vua.AddressType()),
            pa.array([bytes, bytes], type=pa.binary(16)),
            pa.array([24, 25], type=pa.uint8()),
        ],
        names=["address", "length"],
    )
    arr = pa.ExtensionArray.from_storage(ty, storage)
    arr.validate()
    assert arr.type is ty
    assert arr.storage.equals(storage)
    assert arr[0].as_py() == ipaddress.IPv4Network("10.1.21.0/24")
    assert arr[1].as_py() == ipaddress.IPv4Network("10.1.21.0/25")


def test_enum_extension_type():
    fields = {
        1: "foo",
        2: "bar",
        3: "baz",
    }
    ty = vua.EnumType(fields)
    pa.register_extension_type(ty)
    storage = pa.array([1, 2, 3, 2, 1], pa.uint32())
    arr = pa.ExtensionArray.from_storage(ty, storage)
    arr.validate()
    assert arr.type is ty
    assert arr.storage.equals(storage)
    assert arr.to_pylist() == ["foo", "bar", "baz", "bar", "foo"]


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
