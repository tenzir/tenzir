from datetime import datetime, timedelta
import ipaddress
import uuid

import pandas as pd
import stix2

# The namespace UUID for STIX.
STIX_NAMESPACE_UUID = uuid.UUID("00abedb4-aa42-466c-9c01-fed23315a9b7")

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


# The STIX 2.1 bridge that process STIX objects on the fabric.
class STIX:
    """A bridge for translating STIX objects."""

    def __init__(self):
        self.store = stix2.MemoryStore()
        self.identities = {}
        self.identities["vast"] = stix2.Identity(name="VAST", identity_class="system")
        self.identities["zeek"] = stix2.Identity(name="Zeek", identity_class="system")

    # TODO: Do not hardcode the Zeek schema in here, but rather operate with an
    # event taxonomy.
    def make_sighting_from_flows(self, indicator: stix2.Indicator, zeek_tables):
        """Translate zeek connection logs into Network Traffic sightings."""
        events = []
        for table in zeek_tables:
            events.extend(table.to_pylist())
        scos = []
        observed_data = []
        # TODO: aggregate flows into a single Observed Data SDO when the 5-tuple
        # is unique, and then bump number_observed by the multiplicity.
        for event in events:
            # Create Network Traffic SDO.
            # TODO: cast the Arrow Table properly to avoid having to do this
            # conversion here. This should already be a typed data frame.
            src = to_addr_sdo(event["id.orig_h"])
            dst = to_addr_sdo(event["id.resp_h"])
            start = pd.to_datetime(event["ts"])
            duration = event["duration"]
            end = start + duration if duration else None
            flow = stix2.NetworkTraffic(
                start=start,
                end=end,
                is_active=end is None,
                protocols=[event["proto"], event["service"]],
                src_port=event["id.orig_p"],
                dst_port=event["id.resp_p"],
                src_byte_count=event["orig_ip_bytes"],
                dst_byte_count=event["resp_ip_bytes"],
                src_packets=event["orig_pkts"],
                dst_packets=event["resp_pkts"],
                src_ref=src,
                dst_ref=dst,
            )
            scos.append(flow)
            scos.append(src)
            scos.append(dst)
            # Wrap in Observed Data SDO.
            observable = stix2.ObservedData(
                # Figure out what system observed the data, e.g., Zeek.
                # created_by_ref=...,
                first_observed=start,
                last_observed=end,
                number_observed=1,
                object_refs=flow,
            )
            observed_data.append(observable)
        # TODO: Derive the identity from the telemetry.
        # Reference in Sighting SRO.
        sighting = stix2.Sighting(
            created_by_ref=self.identities["zeek"],
            sighting_of_ref=indicator,
            observed_data_refs=observed_data,
        )
        bundle = stix2.Bundle(
            self.identities["vast"],
            self.identities["zeek"],
            sighting,
            *observed_data,
            *scos,
        )
        return bundle
