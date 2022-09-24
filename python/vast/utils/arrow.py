import json
import ipaddress as ip
from typing import SupportsBytes

import pyarrow as pa

# Accepts a 128-bit buffer holding an IPv6 address and returns an IPv4 or IPv6
# address.
def unpack_ip(buffer: SupportsBytes) -> ip.IPv4Address | ip.IPv6Address:
    num = int.from_bytes(buffer, byteorder="big")
    # Convert IPv4 mapped addresses back to regular IPv4.
    # https://tools.ietf.org/html/rfc4291#section-2.5.5.2
    if (num >> 32) == 65535:
        num = num - (65535 << 32)
    return ip.ip_address(num)


class AddressScalar(pa.ExtensionScalar):
    def as_py(self) -> ip.IPv4Address | ip.IPv6Address:
        return unpack_ip(self.value.as_py())


class AddressType(pa.ExtensionType):
    def __init__(self):
        pa.ExtensionType.__init__(self, pa.binary(16), "vast.address")

    def __arrow_ext_serialize__(self) -> bytes:
        return b""

    @classmethod
    def __arrow_ext_deserialize__(self, storage_type, serialized: bytes):
        return AddressType()

    def __reduce__(self):
        return AddressScalar, ()

    def __arrow_ext_scalar_class__(self):
        return AddressScalar


class SubnetScalar(pa.ExtensionScalar):
    def as_py(self) -> ip.IPv4Network | ip.IPv6Network:
        buffer = self.value.as_py()
        network = unpack_ip(buffer[0:16])
        length = buffer[16]
        return ip.ip_network((network, length), strict=False)


class SubnetType(pa.ExtensionType):
    def __init__(self):
        pa.ExtensionType.__init__(self, pa.binary(17), "vast.subnet")

    def __arrow_ext_serialize__(self) -> bytes:
        return b""

    @classmethod
    def __arrow_ext_deserialize__(self, storage_type, serialized: bytes):
        return SubnetType()

    def __reduce__(self):
        return SubnetScalar, ()

    def __arrow_ext_scalar_class__(self):
        return SubnetScalar


class EnumScalar(pa.ExtensionScalar):
    def as_py(self) -> str:
        return self.type.field(self.value.as_py())


class EnumType(pa.ExtensionType):
    def __init__(self, fields: dict[int, str]):
        self._fields = fields
        pa.ExtensionType.__init__(self, pa.uint32(), "vast.enum")

    @property
    def fields(self):
        return self._fields

    def field(self, key):
        return self._fields[key]

    def __arrow_ext_serialize__(self) -> bytes:
        return json.dumps(self._fields).encode()

    @classmethod
    def __arrow_ext_deserialize__(self, storage_type, serialized: bytes):
        fields = json.loads(serialized.decode())
        return EnumType(fields)

    def __reduce__(self):
        return EnumScalar, ()

    def __arrow_ext_scalar_class__(self):
        return EnumScalar


# TODO: move to appropriate location
pa.register_extension_type(AddressType())
pa.register_extension_type(SubnetType())


def names(schema: pa.Schema):
    meta = schema.metadata
    return [meta[key].decode() for key in meta if key.startswith(b"VAST:name:")]


def name(schema: pa.Schema):
    xs = names(schema)
    return xs[0] if xs[0] else ""
