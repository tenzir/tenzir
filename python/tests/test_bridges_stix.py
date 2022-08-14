import ipaddress
import uuid

import pytest
import stix2

import vast.bridges.stix as vbs


def test_ipv4_address_conversion():
    bytes = b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\n\t\x1d\x86"
    addr = ipaddress.IPv6Address(bytes)
    assert addr.ipv4_mapped
    sdo = vbs.to_addr_sdo(addr)
    assert type(sdo) == stix2.IPv4Address
    assert sdo.value == "10.9.29.134"


def test_ipv6_address_conversion():
    link_local = "fe80::1ff:fe23:4567:890a"
    addr = ipaddress.IPv6Address(link_local)
    sdo = vbs.to_addr_sdo(addr)
    assert type(sdo) == stix2.IPv6Address
    assert sdo.value == link_local


def test_stix_uuid_creation():
    id = vbs.make_uuid("test")
    assert id == uuid.UUID("a44625ea-edd0-5353-9d39-767fbcf4d6af")


def test_stix_id_conversion():
    uid = "ced31cd4-bdcb-537d-aefa-92d291bfc11d"
    stix_id = f"file--{uid}"
    assert vbs.uuid_from_id(stix_id) == uuid.UUID(uid)


#def test_incremental_object_extraction(sighting):
#    id = "file--ced31cd4-bdcb-537d-aefa-92d291bfc11d"
#    bridge = vbs.STIX()
#    bridge.store.add(sighting)
#     assert sighting == "foo"
