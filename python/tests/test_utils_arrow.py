import ipaddress
import json

import pyarrow as pa

import tenzir.utils.arrow as vua


def test_ip_ip_extension_type():
    ty = vua.IPType()
    arr = vua.extension_array(["10.1.21.165", None], ty)
    arr.validate()
    assert arr.type is ty
    assert arr[0].as_py() == ipaddress.IPv4Address("10.1.21.165")
    assert arr[1].as_py() == None


def test_subnet_extension_type():
    ty = vua.SubnetType()
    arr = vua.extension_array(["10.1.21.0/24", None, "10.1.20.0/25"], ty)
    arr.validate()
    assert arr.type is ty
    assert arr[0].as_py() == ipaddress.IPv4Network("10.1.21.0/24")
    assert arr[1].as_py() == None
    assert arr[2].as_py() == ipaddress.IPv4Network("10.1.20.0/25")


def test_py_dict_to_arrow_dict():
    py_dict = {
        "foo": 0,
        "bar": 2,
    }
    arrow_dict = vua.py_dict_to_arrow_dict(py_dict)
    arrow_dict.validate()
    assert arrow_dict.to_pylist() == ["foo", None, "bar"]


def test_enum_extension_type():
    fields = {
        "foo": 1,
        "bar": 2,
        "baz": 4,
    }
    enum_py = ["foo", "bar", "baz", None, "foo"]
    ty = vua.EnumType(fields)
    assert ty.__arrow_ext_serialize__().decode() == json.dumps(fields)
    dictionary_type = pa.dictionary(
        vua.EnumType.DICTIONARY_INDEX_TYPE, pa.string(), ordered=False,
    )
    assert vua.EnumType.ext_type == dictionary_type
    arr = vua.extension_array(enum_py, ty)
    arr.validate()
    assert arr.type is ty
    assert arr.to_pylist() == enum_py


def test_extension_type_in_struct():
    src_ips = ["10.1.0.2", None, "10.1.0.4"]
    dst_ips = ["10.2.0.2", None, None]
    srcs = vua.extension_array(src_ips, vua.IPType())
    dsts = vua.extension_array(dst_ips, vua.IPType())

    struct_array = pa.StructArray.from_arrays(
        [srcs, dsts],
        names=["src", "dst"],
    )

    assert struct_array.to_pylist() == [
        {
            "src": None if s is None else ipaddress.ip_address(s),
            "dst": None if d is None else ipaddress.ip_address(d),
        }
        for (s, d) in zip(src_ips, dst_ips)
    ]


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
    # Create sample IP addresses.
    addresses = [
        ipaddress.IPv4Address("10.1.21.165"),
        ipaddress.IPv6Address("2001:200:e000::100"),
    ]
    ip_type = vua.IPType()
    ip_array = vua.extension_array(addresses, ip_type)
    # Create sample subnets.
    networks = [
        ipaddress.IPv4Network("10.1.21.0/24"),
        ipaddress.IPv6Network("2001:200:e000::/35"),
    ]
    subnet_type = vua.SubnetType()
    subnet_array = vua.extension_array(networks, subnet_type)
    # Create sample enums.
    fields = {
        "foo": 1,
        "bar": 2,
    }
    enums = [None, "bar"]
    enum_type = vua.EnumType(fields)
    enum_array = vua.extension_array(enums, enum_type)
    # Assemble a record batch.
    schema = pa.schema([("a", ip_type), ("s", subnet_type), ("e", enum_type)])
    batch = pa.record_batch([ip_array, subnet_array, enum_array], schema=schema)

    # Perform a roundtrip (logic lifted from Arrow's test_extension_type.py.
    buf = ipc_write_batch(batch)
    del batch
    batch = ipc_read_batch(buf)

    # Validate addresses.
    a = batch.column("a")
    assert isinstance(a, pa.ExtensionArray)
    assert a.type == ip_type
    assert a.to_pylist() == addresses
    # Validate subnets.
    s = batch.column("s")
    assert isinstance(s, pa.ExtensionArray)
    assert s.type == subnet_type
    assert s.to_pylist() == networks
    # Validate enums.
    e = batch.column("e")
    assert isinstance(e, pa.ExtensionArray)
    assert e.type == enum_type
    assert e.to_pylist() == enums


def test_schema_name_extraction():
    # Since Arrow cannot attach names to schemas, we do this via metadata.
    schema = pa.schema(
        [("a", "string"), ("b", "string")], metadata={"TENZIR:name:0": "foo"},
    )
    assert vua.name(schema) == "foo"


def test_schema_alias_extraction():
    # Since Arrow cannot attach names to schemas, we do this via metadata.
    schema = pa.schema(
        [("a", "string"), ("b", "string")],
        metadata={"TENZIR:name:0": "foo", "TENZIR:name:1": "bar"},
    )
    names = vua.names(schema)
    assert len(names) == 2
    assert names[0] == "foo"
    assert names[1] == "bar"
    # The first name is the top-level type name.
    assert vua.name(schema) == "foo"


def test_pack_ipv4():
    packed = vua.pack_ip(ipaddress.IPv4Address("10.1.21.165"))
    expected = b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\n\x01\x15\xa5"
    assert packed == expected


def test_pack_ipv6():
    packed = vua.pack_ip(ipaddress.IPv6Address("fe80::7645:6de2:ff:1"))
    expected = b"\xfe\x80\x00\x00\x00\x00\x00\x00\x76\x45\x6d\xe2\x00\xff\x00\x01"
    assert packed == expected


def test_unpack_ip():
    bytes = b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\n\x01\x15\xa5"
    assert vua.unpack_ip(bytes) == ipaddress.IPv4Address("10.1.21.165")


def test_pack_subnetv4():
    expected = b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\n\x01\x00\x00"
    from_obj = vua.pack_subnet(ipaddress.IPv4Network("10.1.0.0/16"))
    assert from_obj[0] == expected
    assert from_obj[1] == 112
    from_str = vua.pack_subnet("10.1.0.0/16")
    assert from_str[0] == expected
    assert from_str[1] == 112


def test_pack_subnetv6():
    expected = b"\x20\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
    from_obj = vua.pack_subnet(ipaddress.IPv6Network("2001::/16"))
    assert from_obj[0] == expected
    assert from_obj[1] == 16
    from_str = vua.pack_subnet("2001:0000::/16")
    assert from_str[0] == expected
    assert from_str[1] == 16
