import json
import ipaddress
import pyarrow as pa

import vast.utils.arrow as vua


def test_pattern_extension_type():
    ty = vua.PatternType()
    storage = pa.array(["/foo*bar/", "/ba.qux/"], pa.string())
    arr = pa.ExtensionArray.from_storage(ty, storage)
    arr.validate()
    assert arr.type is ty
    assert arr.storage.equals(storage)
    assert arr[0].as_py() == "/foo*bar/"
    assert arr[1].as_py() == "/ba.qux/"


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
        "foo": 1,
        "bar": 2,
        "baz": 3,
    }
    ty = vua.EnumType(fields)
    assert ty.__arrow_ext_serialize__().decode() == json.dumps(fields)
    storage = pa.array([1, 2, 3, 2, 1], pa.uint8())
    arr = pa.ExtensionArray.from_storage(ty, storage)
    arr.validate()
    assert arr.type is ty
    assert arr.storage.equals(storage)
    assert arr.to_pylist() == ["foo", "bar", "baz", "bar", "foo"]


# Directly lifted from Arrow's test_extension_type.py
def ipc_write_batch(batch):
    stream = pa.BufferOutputStream()
    writer = pa.RecordBatchStreamWriter(stream, batch.schema)
    writer.write_batch(batch)
    writer.close()
    return stream.getvalue()


# Directly lifted from Arrow's test_extension_type.py
def ipc_read_batch(buf):
    reader = pa.RecordBatchStreamReader(buf)
    return reader.read_next_batch()


def test_ipc():
    # Create sample patterns.
    patterns = ["/foo/", "/bar/"]
    pattern_type = vua.PatternType()
    pattern_storage = pa.array(patterns, type=pa.string())
    pattern_array = pa.ExtensionArray.from_storage(pattern_type, pattern_storage)
    # Create sample IP addresses.
    addresses = [
        b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\n\x01\x15\xa5",
        b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\n\x01\x15\xa6",
    ]
    address_type = vua.AddressType()
    address_storage = pa.array(addresses, type=pa.binary(16))
    address_array = pa.ExtensionArray.from_storage(address_type, address_storage)
    # Create sample subnets.
    network = b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\n\x01\x15\x00"
    networks = [network, network]
    prefixes = [24, 25]
    subnet_type = vua.SubnetType()
    subnet_storage = pa.StructArray.from_arrays(
        [
            pa.array(networks, type=pa.binary(16)),
            pa.array(prefixes, type=pa.uint8()),
        ],
        names=["address", "length"],
    )
    subnet_array = pa.ExtensionArray.from_storage(subnet_type, subnet_storage)
    # Create sample enums.
    fields = {
        "foo": 1,
        "bar": 2,
    }
    enum_type = vua.EnumType(fields)
    # Note: if we do not register the type prior to performing IPC, the
    # roundtrip fails.
    pa.register_extension_type(enum_type)
    enum_storage = pa.array([2, 1], pa.uint8())
    enum_array = pa.ExtensionArray.from_storage(enum_type, enum_storage)
    # Assemble a record batch.
    schema = pa.schema(
        [("p", pattern_type), ("a", address_type), ("s", subnet_type), ("e", enum_type)]
    )
    batch = pa.record_batch(
        [pattern_array, address_array, subnet_array, enum_array], schema=schema
    )
    # Perform a roundtrip (logic lifted from Arrow's test_extension_type.py.
    buf = ipc_write_batch(batch)
    del batch
    batch = ipc_read_batch(buf)
    # Validate patterns.
    p = batch.column("p")
    assert isinstance(p, pa.ExtensionArray)
    assert p.type == pattern_type
    assert p.storage.to_pylist() == patterns
    # Validate addresses.
    a = batch.column("a")
    assert isinstance(a, pa.ExtensionArray)
    assert a.type == address_type
    assert a.storage.to_pylist() == addresses
    # Validate subnets.
    s = batch.column("s")
    assert isinstance(s, pa.ExtensionArray)
    assert s.type == subnet_type
    assert s.storage.field("address").to_pylist() == networks
    assert s.storage.field("length").to_pylist() == prefixes
    # Validate enums.
    e = batch.column("e")
    assert isinstance(e, pa.ExtensionArray)
    assert e.type == enum_type
    assert e.storage.equals(enum_storage)


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


def test_unpack_ip():
    bytes = b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\n\x01\x15\xa5"
    assert vua.unpack_ip(bytes) == ipaddress.IPv4Address("10.1.21.165")
