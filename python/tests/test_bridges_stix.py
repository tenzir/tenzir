import ipaddress

import pytest
import stix2


import vast.bridges.stix as stix


def test_ipv4_address_conversion():
    bytes = b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\n\t\x1d\x86'
    addr = ipaddress.IPv6Address(bytes)
    assert addr.ipv4_mapped
    sdo = stix.to_addr_sdo(addr)
    assert type(sdo) == stix2.IPv4Address
    assert sdo.value == "10.9.29.134"

def test_ipv6_address_conversion():
    link_local = "fe80::1ff:fe23:4567:890a"
    addr = ipaddress.IPv6Address(link_local)
    sdo = stix.to_addr_sdo(addr)
    assert type(sdo) == stix2.IPv6Address
    assert sdo.value == link_local
