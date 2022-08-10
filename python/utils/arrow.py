import ipaddress

import pyarrow as pa


class IPAddressType(pa.PyExtensionType):
    def __init__(self):
        pa.PyExtensionType.__init__(self, pa.binary(16))

    def __reduce__(self):
        return IPAddressType, ()

    def __arrow_ext_scalar_class__(self):
        # TODO: we should probably write our own IP address type that supports
        # the v4-in-v6 embedding natively.
        return ipaddress.IPv6Address

def names(table: pa.Table):
    meta = table.schema.metadata
    return [meta[key].decode() for key in meta if key.startswith(b"VAST:name:")]


def name(table: pa.Table):
    xs = names(table)
    return xs[0] if xs[0] else ""

