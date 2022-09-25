import json
import ipaddress as ip
from typing import SupportsBytes

import pyarrow as pa


class PatternScalar(pa.ExtensionScalar):
    def as_py(self) -> str:
        return self.value.as_py()


class PatternType(pa.ExtensionType):
    ext_name = "vast.pattern"
    ext_type = pa.string()

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

    def __reduce__(self):
        return PatternScalar, ()

    def __arrow_ext_scalar_class__(self):
        return PatternScalar


class AddressScalar(pa.ExtensionScalar):
    def as_py(self) -> ip.IPv4Address | ip.IPv6Address:
        return unpack_ip(self.value.as_py())


class AddressType(pa.ExtensionType):
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

    def __reduce__(self):
        return AddressScalar, ()

    def __arrow_ext_scalar_class__(self):
        return AddressScalar


class SubnetScalar(pa.ExtensionScalar):
    def as_py(self) -> ip.IPv4Network | ip.IPv6Network:
        address = unpack_ip(self.value[0].as_py())
        length = self.value[1].as_py()
        return ip.ip_network((address, length), strict=False)


class SubnetType(pa.ExtensionType):
    ext_name = "vast.subnet"
    # This gives us a pyarrow.lib.ArrowNotImplementedError :-/
    # ext_type = pa.struct([("address", AddressType()),
    #                       ("length", pa.uint8())])
    ext_type = pa.struct([("address", pa.binary(16)), ("length", pa.uint8())])

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
    def as_py(self) -> str:
        return self.type.field(self.value.as_py())


class EnumType(pa.ExtensionType):
    ext_name = "vast.enumeration"
    ext_type = pa.uint32()

    def __init__(self, fields: dict[int, str]):
        self._fields = fields
        pa.ExtensionType.__init__(self, self.ext_type, self.ext_name)

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


# Accepts a 128-bit buffer holding an IPv6 address and returns an IPv4 or IPv6
# address.
def unpack_ip(buffer: SupportsBytes) -> ip.IPv4Address | ip.IPv6Address:
    num = int.from_bytes(buffer, byteorder="big")
    # Convert IPv4 mapped addresses back to regular IPv4.
    # https://tools.ietf.org/html/rfc4291#section-2.5.5.2
    if (num >> 32) == 65535:
        num = num - (65535 << 32)
    return ip.ip_address(num)


# Modules are intialized exactly once, so we can perform the registration here.
pa.register_extension_type(PatternType())
pa.register_extension_type(AddressType())
pa.register_extension_type(SubnetType())
