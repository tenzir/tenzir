import json
import ipaddress as ip
from typing import Iterable, Optional, Sequence, SupportsBytes, SupportsIndex

import pyarrow as pa


class IPScalar(pa.ExtensionScalar):
    def as_py(self) -> ip.IPv4Address | ip.IPv6Address | None:
        return None if self.value is None else unpack_ip(self.value.as_py())


class IPType(pa.ExtensionType):
    # NOTE: The identifier for the extension type of VAST's ip type has not
    # changed when the type was renamed from address to ip because that would be
    # a breaking change. This is fixable by registering two separate extension
    # types with the same functionality but different ids, but that's a lot of
    # effort for something users don't usually see.
    ext_name = "vast.address"
    ext_type = pa.binary(16)

    def __init__(self):
        pa.ExtensionType.__init__(self, self.ext_type, self.ext_name)

    def __arrow_ext_serialize__(self) -> bytes:
        return self.ext_name.encode()

    @classmethod
    def __arrow_ext_deserialize__(self, storage_type, serialized: bytes):
        if serialized.decode() != self.ext_name:
            raise TypeError("type identifier does not match")
        if storage_type != self.ext_type:
            raise TypeError("storage type does not match")
        return IPType()

    def __reduce__(self):
        return IPScalar, ()

    def __arrow_ext_scalar_class__(self):
        return IPScalar


class SubnetScalar(pa.ExtensionScalar):
    def as_py(self) -> ip.IPv4Network | ip.IPv6Network | None:
        address = self.value[0].as_py()
        length = self.value[1].as_py()
        if address is None or length is None:
            return None
        return ip.ip_network((address, length), strict=False)


class SubnetType(pa.ExtensionType):
    ext_name = "vast.subnet"
    ext_type = pa.struct([("address", IPType()), ("length", pa.uint8())])

    def __init__(self):
        pa.ExtensionType.__init__(self, self.ext_type, self.ext_name)

    def __arrow_ext_serialize__(self) -> bytes:
        return self.ext_name.encode()

    @classmethod
    def __arrow_ext_deserialize__(self, storage_type, serialized: bytes):
        if serialized.decode() != self.ext_name:
            raise TypeError("type identifier does not match")
        if storage_type != self.ext_type:
            raise TypeError("storage type does not match")
        return SubnetType()

    def __reduce__(self):
        return SubnetScalar, ()

    def __arrow_ext_scalar_class__(self):
        return SubnetScalar


class EnumScalar(pa.ExtensionScalar):
    def as_py(self) -> str | None:
        return None if self.value is None else self.value.as_py()


class EnumType(pa.ExtensionType):
    """An extension class for enum data types.

    `EnumType` stores as name-to-value mapping called `fields` that maps to the
    underlying integer type of represented the enum."""

    # VAST's flatbuffer type representation uses a 32-bit unsigned integer. We
    # use an 8-bit type here only for backwards compatibility to the legacy
    # type. Eventually this will be a 32-bit type as well.
    DICTIONARY_INDEX_TYPE = pa.uint8()

    ext_name = "vast.enumeration"
    ext_type = pa.dictionary(DICTIONARY_INDEX_TYPE, pa.string())

    def __init__(self, fields: dict[str, int]):
        self._fields = fields
        pa.ExtensionType.__init__(self, self.ext_type, self.ext_name)

    @property
    def fields(self) -> dict[str, int]:
        return self._fields

    def __arrow_ext_serialize__(self) -> bytes:
        return json.dumps(self._fields).encode()

    @classmethod
    def __arrow_ext_deserialize__(self, storage_type, serialized: bytes):
        fields = json.loads(serialized.decode())
        if storage_type != self.ext_type:
            raise TypeError("storage type does not match")
        return EnumType(fields)

    def __reduce__(self):
        return EnumScalar, ()

    def __arrow_ext_scalar_class__(self):
        return EnumScalar


def names(schema: pa.Schema):
    meta = schema.metadata
    return [meta[key].decode() for key in meta if key.startswith(b"VAST:name:")]


def name(schema: pa.Schema):
    xs = names(schema)
    return xs[0] if xs[0] else ""


def pack_ip(address: str | ip.IPv4Address | ip.IPv6Address) -> bytes:
    match address:
        case str():
            return pack_ip(ip.ip_address(address))
        case ip.IPv4Address():
            prefix = b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff"
            return prefix + address.packed
        case ip.IPv6Address():
            return address.packed


# Accepts a 128-bit buffer holding an IPv6 address and returns an IPv4 or IPv6
# address.
def unpack_ip(
    buffer: Iterable[SupportsIndex] | SupportsBytes,
) -> ip.IPv4Address | ip.IPv6Address:
    num = int.from_bytes(buffer, byteorder="big")
    # Convert IPv4 mapped addresses back to regular IPv4.
    # https://tools.ietf.org/html/rfc4291#section-2.5.5.2
    if (num >> 32) == 65535:
        num = num - (65535 << 32)
    return ip.ip_address(num)


def pack_subnet(
    subnet: str | ip.IPv4Network | ip.IPv6Network | None,
) -> tuple[bytes | None, int | None]:
    if subnet is None:
        return (None, None)
    match subnet:
        case str():
            return pack_subnet(ip.ip_network(subnet))
        case ip.IPv4Network():
            prefix = b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff"
            return (prefix + subnet.network_address.packed, subnet.prefixlen)
        case ip.IPv6Network():
            return (subnet.network_address.packed, subnet.prefixlen)


VastExtensionType = IPType | EnumType | SubnetType


def py_dict_to_arrow_dict(dict: dict[str, int]) -> pa.StringArray:
    """Build an Arrow dictionary array from a Python dictionary

    - Values should be greater or equal to 0.
    - Non-consecutive values will interpolated with null."""
    dictionary_py: list[Optional[str]] = [None] * (max(dict.values()) + 1)
    for (value, idx) in dict.items():
        assert idx >= 0, f"Dictionary indices should be >=0, got {idx}"
        dictionary_py[idx] = value
    return pa.array(dictionary_py, pa.string())


def extension_array(obj: Sequence, type: pa.DataType) -> pa.Array:
    """Create a `pyarrow.Array` from a Python object, using an
    `pyarrow.ExtensionArray` if the type is an extension type."""
    match type:
        case IPType():
            arr = [pack_ip(e) for e in obj]
            storage = pa.array(arr, pa.binary(16))
            return pa.ExtensionArray.from_storage(type, storage)
        case SubnetType():
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
            return pa.ExtensionArray.from_storage(type, storage)
        case EnumType(fields=fields):
            # use the mappings in the `fields` metadata as indices for building
            # the dictionary
            indices_py = [None if e is None else fields[e] for e in obj]
            indices = pa.array(indices_py, EnumType.DICTIONARY_INDEX_TYPE)
            dictionary = py_dict_to_arrow_dict(fields)
            storage = pa.DictionaryArray.from_arrays(indices, dictionary)
            return pa.ExtensionArray.from_storage(type, storage)
        case _:
            return pa.array(obj, type)


# Modules are intialized exactly once, so we can perform the registration here.
pa.register_extension_type(IPType())
pa.register_extension_type(SubnetType())
pa.register_extension_type(EnumType({"stub": 0}))
