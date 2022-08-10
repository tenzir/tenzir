from datetime import datetime, timedelta

import stix2

# The STIX 2.1 bridge that process STIX objects on the fabric.
class STIX:
    def __init__(self):
        self.store = stix2.MemoryStore()
        self.identity = stix2.Identity(name="VAST", identity_class="system")
        self.store.add(self.identity)

    def make_sighting(self, indicator: stix2.Indicator, events):
        scos = []
        observed_data = []
        # TODO: Go beyond Zeek conn logs. We should use concepts or type aliases
        # to make this mapping process easier.
        for event in events:
            # Create Network Traffic SDO.
            src = stix2.IPv4Address(value=event["id.orig_h"])
            dst = stix2.IPv4Address(value=event["id.resp_h"])
            start = datetime.fromisoformat(event["ts"].split('.')[0])
            duration = timedelta(seconds=float(event["duration"]))
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
                    #created_by_ref=...,
                    first_observed=start,
                    last_observed=end,
                    number_observed=1,
                    object_refs=flow)
            observed_data.append(observable)
        # Reference in Sighting SRO.
        sighting = stix2.Sighting(
                # TODO: use the inner-most data source and not VAST itself.
                created_by_ref=self.identity,
                sighting_of_ref=indicator,
                observed_data_refs=observed_data)
        bundle = stix2.Bundle(self.identity, sighting, *observed_data, *scos)
        return bundle
