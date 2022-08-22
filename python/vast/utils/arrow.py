import ipaddress
from typing import SupportsBytes

import pyarrow as pa
import pandas as pd


def names(schema: pa.Schema):
    meta = schema.metadata
    return [meta[key].decode() for key in meta if key.startswith(b"VAST:name:")]


def name(schema: pa.Schema):
    xs = names(schema)
    return xs[0] if xs[0] else ""


# Accepts a 128-bit buffer holding an IPv6 address and
# returns an IPv4 or IPv6 address.
def unpack_ip(buffer: SupportsBytes) -> str:
    num = int.from_bytes(buffer, byteorder="big")
    # convert IPv4 mapped addresses back to regular IPv4
    # https://tools.ietf.org/html/rfc4291#section-2.5.5.2
    if (num >> 32) == 65535:
        num = num - (65535 << 32)
    return ipaddress.ip_address(num)


def fmt_ip(df: pd.DataFrame, ip_columns: list[str]) -> pd.DataFrame:
    """Formats IP of a dataframe Pass columns to format as arguments."""
    columns = []
    for col_name in df.keys():
        if col_name in ip_columns:
            columns.append(df[col_name].apply(unpack_ip))
        else:
            columns.append(df[col_name])
    return pd.concat(columns, axis=1)
