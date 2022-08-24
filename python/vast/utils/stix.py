import ipaddress
import uuid

import stix2

# The namespace UUID for STIX.
STIX_NAMESPACE_UUID = uuid.UUID("00abedb4-aa42-466c-9c01-fed23315a9b7")

# The identity of a VAST node.
VAST_IDENTITY = stix2.Identity(name="VAST", identity_class="system")

def to_addr_sdo(data):
    """Translate a potentially ipv4-mapped IPv6 address into the right IP
    address SCO."""
    addr = ipaddress.IPv6Address(data)
    if addr.ipv4_mapped:
        return stix2.IPv4Address(value=addr.ipv4_mapped)
    else:
        return stix2.IPv6Address(value=addr)


def make_uuid(name: str):
    """Make a STIX 2.1+ compliant UUIDv5 from a "name"."""
    return uuid.uuid5(STIX_NAMESPACE_UUID, name)


def uuid_from_id(stix_id: str):
    """Convert a STIX ID to a UUID."""
    idx = stix_id.index("--")
    uuid_str = stix_id[idx + 2 :]
    return uuid.UUID(uuid_str)
