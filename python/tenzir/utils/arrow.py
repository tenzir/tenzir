import ipaddress as ip
import sys
import json
import itertools
import copy
from datetime import datetime, timedelta
from types import MappingProxyType
from typing import Iterable, Optional, Sequence, SupportsBytes, SupportsIndex, Union

import pyarrow as pa

try:
    from pandas import Timestamp, Timedelta
except ImportError:
    # Create dummy classes so we can safely test
    # `isinstance(x, Timestamp)` below.
    class Timestamp():
        pass
    class Timedelta():
        pass

class IPScalar(pa.ExtensionScalar):
    def as_py(self: "IPScalar") -> Union[ip.IPv4Address, ip.IPv6Address, None]:
        return None if self.value is None else unpack_ip(self.value.as_py())


class IPType(pa.ExtensionType):
    ext_name = "tenzir.ip"
    ext_type = pa.binary(16)

    def __init__(self: "IPType") -> None:
        pa.ExtensionType.__init__(self, self.ext_type, self.ext_name)

    def __arrow_ext_serialize__(self: "IPType") -> bytes:
        return self.ext_name.encode()

    @classmethod
    def __arrow_ext_deserialize__(
        cls: pa.ExtensionType,
        storage_type: pa.FixedSizeBinaryType,
        serialized: bytes,
    ) -> "IPType":
        if serialized.decode() != cls.ext_name:
            msg = "type identifier does not match"
            raise TypeError(msg)
        if storage_type != cls.ext_type:
            msg = "storage type does not match"
            raise TypeError(msg)
        return IPType()

    def __reduce__(self: "IPType") -> tuple[type[IPScalar], tuple[()]]:
        return IPScalar, ()

    def __arrow_ext_scalar_class__(self: "IPType") -> type[IPScalar]:
        return IPScalar


class SubnetScalar(pa.ExtensionScalar):
    def as_py(self: "SubnetScalar") -> Union[ip.IPv4Network, ip.IPv6Network, None]:
        address = self.value[0].as_py()
        length = self.value[1].as_py()
        if address is None or length is None:
            return None
        # The ip encoding of tenzir represents all IPv4 addresses as IPv4 mapped in the
        # IPv6 binary representation. In case we have such a mapped address the type of
        # address will be v4 here. But if the prefix is below 96 we must assume that the
        # address was originally a v6, so we convert it to that representation.
        # This fixes cases like `::ffff:1234:0/112`.
        if address.version == 4:
            if length < 96:
                # This fixes cases like `::ffff:1234:0/80`, which is not a valid
                # IPv4 network.
                address = ip.IPv6Address((65535 << 32) + int(address))
            else:
                # This fixes cases like `::ffff:1234:0/112`, which is
                # indistinguishable from `18.52.0.0/16`.
                length = length - 96
        return ip.ip_network((address, length), strict=False)


class SubnetType(pa.ExtensionType):
    ext_name = "tenzir.subnet"
    ext_type = pa.struct([("address", IPType()), ("length", pa.uint8())])

    def __init__(self: "SubnetType") -> None:
        pa.ExtensionType.__init__(self, self.ext_type, self.ext_name)

    def __arrow_ext_serialize__(self: "SubnetType") -> bytes:
        """Explain how to serialize the type metadata."""
        return self.ext_name.encode()

    @classmethod
    def __arrow_ext_deserialize__(
        cls: pa.ExtensionType,
        storage_type: pa.StructType,
        serialized: bytes,
    ) -> "SubnetType":
        if serialized.decode() != cls.ext_name:
            msg = "type identifier does not match"
            raise TypeError(msg)
        if storage_type != cls.ext_type:
            msg = "storage type does not match"
            raise TypeError(msg)
        return SubnetType()

    def __reduce__(self: "SubnetType") -> tuple[type[SubnetScalar], tuple[()]]:
        return SubnetScalar, ()

    def __arrow_ext_scalar_class__(self: "SubnetType") -> type[SubnetScalar]:
        return SubnetScalar


class EnumScalar(pa.ExtensionScalar):
    """Adapter for Tenzir enumeration values."""

    def as_py(self: "EnumScalar") -> Union[str, None]:
        return None if self.value is None else self.value.as_py()


class EnumType(pa.ExtensionType):
    """An extension class for enum data types.

    `EnumType` stores as name-to-value mapping called `fields` that maps to the
    underlying integer type of represented the enum.
    """

    # Tenzir's flatbuffer type representation uses a 32-bit unsigned integer. We
    # use an 8-bit type here only for backwards compatibility to the legacy
    # type. Eventually this will be a 32-bit type as well.
    DICTIONARY_INDEX_TYPE = pa.uint8()

    ext_name = "tenzir.enumeration"
    ext_type = pa.dictionary(DICTIONARY_INDEX_TYPE, pa.string())

    def __init__(self: "EnumType", fields: dict[str, int]) -> None:
        self._fields = fields
        pa.ExtensionType.__init__(self, self.ext_type, self.ext_name)

    @property
    def fields(self: "EnumType") -> dict[str, int]:
        return self._fields

    def __arrow_ext_serialize__(self: "EnumType") -> bytes:
        return json.dumps(self._fields).encode()

    @classmethod
    def __arrow_ext_deserialize__(
        cls: pa.ExtensionType,
        storage_type: pa.DictionaryType,
        serialized: bytes,
    ) -> "EnumType":
        fields = json.loads(serialized.decode())
        if storage_type != cls.ext_type:
            msg = "storage type does not match"
            raise TypeError(msg)
        return EnumType(fields)

    def __reduce__(self: "EnumType") -> tuple[type[EnumScalar], tuple[()]]:
        return EnumScalar, ()

    def __arrow_ext_scalar_class__(self: "EnumType") -> type[EnumScalar]:
        return EnumScalar


def names(schema: pa.Schema) -> list[str]:
    """Return all tenzir type names contained in the schema."""
    meta = schema.metadata
    return [meta[key].decode() for key in meta if key.startswith(b"TENZIR:name:")]


def name(schema: pa.Schema) -> str:
    """Return the schema name."""
    xs = names(schema)
    return xs[0] if xs[0] else ""


def pack_ip(address: Union[str, ip.IPv4Address, ip.IPv6Address]) -> bytes:
    """Convert an ip address to arrow array bytes."""
    if isinstance(address, str):
        return pack_ip(ip.ip_address(address))
    elif isinstance(address, ip.IPv4Address):
        prefix = b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff"
        return prefix + address.packed
    elif isinstance(address, ip.IPv6Address):
        return address.packed


# Accepts a 128-bit buffer holding an IPv6 address and returns an IPv4 or IPv6
# address.
def unpack_ip(
    buffer: Union[Iterable[SupportsIndex], SupportsBytes],
) -> Union[ip.IPv4Address, ip.IPv6Address]:
    """Construct an ip address from a slice of bytes."""
    num = int.from_bytes(buffer, byteorder="big")
    # Convert IPv4 mapped addresses back to regular IPv4.
    # https://tools.ietf.org/html/rfc4291#section-2.5.5.2
    mask = 0xFFFF
    if (num >> 32) == mask:
        return ip.IPv4Address(num - (mask << 32))
    return ip.IPv6Address(num)


def pack_subnet(
    subnet: Union[str, ip.IPv4Network, ip.IPv6Network, None],
) -> tuple[Union[bytes, None], Union[int, None]]:
    """Convert a subnet to arrow array bytes."""
    if subnet is None:
        return (None, None)
    if isinstance(subnet, str):
        return pack_subnet(ip.ip_network(subnet))
    elif isinstance(subnet, ip.IPv4Network):
        prefix = b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff"
        return (prefix + subnet.network_address.packed, subnet.prefixlen + 96)
    elif isinstance(subnet, ip.IPv6Network):
        return (subnet.network_address.packed, subnet.prefixlen)


VastExtensionType = Union[IPType, EnumType, SubnetType]


def py_dict_to_arrow_dict(x: dict[str, int]) -> pa.StringArray:
    """Build an Arrow dictionary array from a Python dictionary.

    - Values should be greater or equal to 0.
    - Non-consecutive values will interpolated with null.
    """
    dictionary_py: list[Optional[str]] = [None] * (max(x.values()) + 1)
    for value, idx in x.items():
        if idx < 0:
            msg = f"Dictionary indices should be >=0, got {idx}"
            raise ValueError(msg)
        dictionary_py[idx] = value
    return pa.array(dictionary_py, pa.string())


def extension_array_scalar(obj: Sequence, datatype: pa.DataType) -> pa.Array:
    """Create a `pyarrow.Array` from a Python object with extension type support."""
    if isinstance(datatype, IPType):
        arr = (pack_ip(e) for e in obj)
        storage = pa.array(arr, pa.binary(16))
        return pa.ExtensionArray.from_storage(datatype, storage)
    elif isinstance(datatype,  SubnetType):
        arrs = list(zip(*[pack_subnet(e) for e in obj]))
        addr_storage = pa.array(arrs[0], pa.binary(16))
        addr_array = pa.ExtensionArray.from_storage(IPType(), addr_storage)
        prefix_array = pa.array(arrs[1], pa.uint8())
        storage = pa.StructArray.from_arrays(
            [
                addr_array,
                prefix_array,
            ],
            names=["address", "length"],
        )
        return pa.ExtensionArray.from_storage(datatype, storage)
    elif isinstance(datatype,  EnumType):
        fields = datatype.fields
        # use the mappings in the `fields` metadata as indices for building
        # the dictionary
        indices_py = [None if e is None else fields[e] for e in obj]
        indices = pa.array(indices_py, EnumType.DICTIONARY_INDEX_TYPE)
        dictionary = py_dict_to_arrow_dict(fields)
        storage = pa.DictionaryArray.from_arrays(indices, dictionary)
        return pa.ExtensionArray.from_storage(datatype, storage)
    else:
        return pa.array(obj, datatype)


def extension_array(obj: Sequence, datatype: pa.DataType) -> pa.Array:
    if isinstance(datatype, pa.StructType):
        arrays = []
        fields = []
        for i in range(datatype.num_fields):
            inner_field = datatype.field(i)
            # 3rd instance `to_pydict()`
            inner_obj = [
                (item[inner_field.name] if item is not None else None) for item in obj
            ]
            print(
                f"making extension array {inner_obj=} {inner_field=} {inner_field.type=}",
                file=sys.stderr,
            )
            fields.append(inner_field)
            arrays.append(extension_array(inner_obj, inner_field.type))
        return pa.StructArray.from_arrays(arrays, fields=fields)
    elif isinstance(datatype, pa.ListType):
        inner_type = datatype.value_type
        # Offset computation according to the rules in [1]
        # [1]: https://arrow.apache.org/docs/python/generated/pyarrow.ListArray.html#pyarrow.ListArray.from_arrays
        offsets: list[Union[int, None]] = []
        last_notnull_offset = 0
        inner_sequence: list[pa.Array] = []
        for lst in obj:
            if lst is None:
                offsets.append(None)
            else:
                offsets.append(last_notnull_offset)
                last_notnull_offset = last_notnull_offset + len(lst)
                inner_sequence = inner_sequence + lst
        offsets.append(last_notnull_offset)
        array = extension_array(inner_sequence, inner_type)
        return pa.ListArray.from_arrays(offsets, array, datatype)
    else:
        return extension_array_scalar(obj, datatype)


TenzirType = Union[
    ip.IPv4Address,
    ip.IPv6Address,
    ip.IPv4Network,
    ip.IPv6Network,
    str,
    bool,
    int,
    float,
    datetime,
    bytes,
    None,
    dict[str, "TenzirType"],
    list["TenzirType"]
]


def infer_type(obj: TenzirType) -> pa.DataType:
    """Map a python type to the corresponding Arrow type.

    Note that the arrow -> python mapping is not completely static,
    for example timestamp[ns] is converted to `pandas.Timestamp` if pandas
    is installed or `datetime.datetime` otherwise. Therefore this function
    can also only give a best-effort estimate of the reverse mapping.
    TODO @tobim: Consider changing that by implementing our own `to_pydict()`
                 alternative.
    """
    # TODO: Use match statement
    if isinstance(obj, (ip.IPv4Address, ip.IPv6Address)):
        return IPType()
    if isinstance(obj, (ip.IPv4Network, ip.IPv6Network)):
        return SubnetType()
    if isinstance(obj, str):
        return pa.string()
    if isinstance(obj, bool):
        return pa.bool_()
    # In python `isinstance(True, int) == True` so this needs to be after
    # the check for bool type.
    if isinstance(obj, int):
        return pa.int64()
    if isinstance(obj, float):
        return pa.float64()
    if isinstance(obj, datetime) or isinstance(obj, Timestamp):
        return pa.timestamp("ns")
    if isinstance(obj, timedelta) or isinstance(obj, Timedelta):
        return pa.duration("ns")
    if obj is None:
        return pa.null()
    if isinstance(obj, bytes):
        return pa.binary()
    if isinstance(obj, dict):
        return pa.struct({key: infer_type(value) for key, value in obj.items()})
    if isinstance(obj, list):
        gen = (x for x in obj if x is not None)
        sample_value = next(gen, None)
        if isinstance(sample_value, dict):
            # The problem with structs is that we'd need to find a non-null sample for
            # every *nested* field in every subrecord or sublist.
            raise Exception(
                "inferring the type of structs in lists is not supported yet"
            )
        return pa.list_(infer_type(sample_value))
    msg = f"cannot convert value of type {type(obj)} back to arrow"
    raise Exception(msg)


def make_record_batch(
    data: dict[str, list],
    type_hints: MappingProxyType[str, type] = MappingProxyType({}),
) -> pa.RecordBatch:
    """Create a record batch from a python object.

    The type of all value must have a corresponding type in the Tenzir type
    system.
    """

    def find_first_nonnull(xs: list[Union[TenzirType, None]]) -> TenzirType | None:
        return next((x for x in xs if x is not None), None)

    arrays = []
    fields = []
    for name, value in data.items():
        if len(value) == 0:
            continue
        type_ = (
            type_hints[name]
            if name in type_hints
            else infer_type(find_first_nonnull(value))
        )
        fields.append(pa.field(name, type_))
        arrays.append(extension_array(value, type_))
    return pa.RecordBatch.from_arrays(arrays=arrays, schema=pa.schema(fields))


# Modules are intialized exactly once, so we can perform the registration here.
pa.register_extension_type(IPType())
pa.register_extension_type(SubnetType())
pa.register_extension_type(EnumType({"stub": 0}))
