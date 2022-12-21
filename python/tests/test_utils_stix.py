import ipaddress
import json
import pathlib
import pytest
import uuid

import stix2

from vast.utils.stix import *


@pytest.fixture
def sighting(request):
    tests = pathlib.Path(request.node.fspath.strpath).parent
    bundle = tests / "data" / "stix-bundle-sighting.json"
    with bundle.open() as f:
        bundle = json.load(f)
        return stix2.parse(bundle)


def test_ipv4_address_conversion():
    bytes = b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\n\t\x1d\x86"
    addr = ipaddress.IPv6Address(bytes)
    assert addr.ipv4_mapped
    sdo = to_addr_sdo(addr)
    assert type(sdo) == stix2.IPv4Address
    assert sdo.value == "10.9.29.134"


def test_ipv6_address_conversion():
    link_local = "fe80::1ff:fe23:4567:890a"
    addr = ipaddress.IPv6Address(link_local)
    sdo = to_addr_sdo(addr)
    assert type(sdo) == stix2.IPv6Address
    assert sdo.value == link_local


def test_stix_uuid_creation():
    id = make_uuid("test")
    assert id == uuid.UUID("a44625ea-edd0-5353-9d39-767fbcf4d6af")


def test_stix_id_conversion():
    uid = "ced31cd4-bdcb-537d-aefa-92d291bfc11d"
    stix_id = f"file--{uid}"
    assert uuid_from_id(stix_id) == uuid.UUID(uid)
