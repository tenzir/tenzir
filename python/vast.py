from datetime import datetime, timedelta
import asyncio
from typing import Any

from dynaconf import Dynaconf
import json
import pyarrow
import stix2

from fabric import Fabric
import backbones.inmemory
import utils.logging

logger = utils.logging.get("node")

class CLI:
    @staticmethod
    def arguments(**kwargs):
        result = []
        for k, v in kwargs.items():
            if v is True:
                result.append(f"--{k.replace('_','-')}")
            else:
                result.append(f"--{k.replace('_','-')}={v}")
        return result

    """Wraps a VAST executable"""
    def __init__(self, **kwargs):
        self.args = CLI.arguments(**kwargs)

    async def exec(self, stdin=False):
        async def run(*args, stdin):
            return await asyncio.create_subprocess_exec(
                "vast",
                *args,
                stdin=asyncio.subprocess.PIPE if stdin else None,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
        logger.debug(f"> vast {' '.join(self.args)}")
        if not stdin:
            return await run(*self.args, stdin=False)
        proc = await run(*self.args, stdin=True)
        proc.stdin.write(str(stdin).encode())
        await proc.stdin.drain()
        proc.stdin.close()
        return proc

    def __getattr__(self, name, **kwargs):
        """Chains every unknown method call to the internal call stack."""
        if name.endswith("_"):
            # trim trailing underscores to overcome the 'import' keyword
            name = name[:-1]
        if not name == "__iter_":
            self.args.append(name.replace("_", "-"))
        def command(*args, **kwargs):
            if kwargs:
                self.args.extend(CLI.arguments(**kwargs))
            if args:
                self.args.extend(args)
            return self
        return command

# The VAST node.
class VAST:
    @staticmethod
    async def create(config: Dynaconf):
        backbone = backbones.inmemory.InMemory()
        fabric = Fabric(config, backbone)
        self = VAST(config, fabric)
        logger.debug("subscribing to STIX topics")
        await self.subscribe("stix.indicator", self._on_stix_indicator)
        return self

    def __init__(self, config: Dynaconf, fabric: Fabric):
        self.config = config
        self.fabric = fabric
        # TODO: Start a matcher
        #cmd = CLI(plugins="matcher").matcher().start(match_types="[:string]").test()

    async def start(self, **kwargs):
        proc = await CLI(**kwargs).start().exec()
        await proc.communicate()
        logger.debug(proc.stderr.decode())
        return proc

    async def publish(self, topic: str, data: Any):
        await self.fabric.publish(topic, data)

    async def subscribe(self, topic: str, callback):
        await self.fabric.subscribe(topic, callback)

    async def _on_stix_indicator(self, message: Any):
        logger.debug(message)
        indicator = stix2.parse(message)
        if type(indicator) != stix2.Indicator:
            logger.warn(f"expected indicator, got {type(indicator)}")
            return
        if indicator.pattern_type == "vast":
            logger.debug(f"got VAST expression {indicator.pattern}")
        elif indicator.pattern_type == "sigma":
            logger.debug(f"got Sigma rule {indicator.pattern}")
        else:
            logger.warn(f"got unsupported pattern type {indicator.pattern_type}")
            return
        results = await self._query(indicator.pattern)
        if len(results) == 0:
            logger.info(f"got no results for query: {indicator.pattern}")
            return
        # Extract SCOs.
        # TODO: Go beyond Zeek conn logs. We should use concepts or type aliases
        # to make this mapping process easier.
        scos = []
        observed_data = []
        for event in results:
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
                    first_observed=start,
                    last_observed=end,
                    number_observed=1,
                    object_refs=flow)
            observed_data.append(observable)
        # Reference in Sighting SRO.
        sighting = stix2.Sighting(
                sighting_of_ref=indicator,
                observed_data_refs=observed_data)
        bundle = stix2.Bundle(sighting, *observed_data, *scos)
        logger.warning(bundle.serialize(pretty=True))
        await self.publish("stix.bundle", bundle)

    async def _query(self, expression: str, limit: int = 100):
        proc = await CLI().export(max_events=limit).json(
                expression,
                numeric_durations=True).exec()
        stdout, stderr = await proc.communicate()
        return [json.loads(line) for line in stdout.decode().splitlines()]
        #logger.debug(stderr.decode())
        #proc = await CLI().export(max_events=limit).arrow(expression).exec()
        #stdout, stderr = await proc.communicate()
        #logger.debug(stderr.decode())
        #istream = pyarrow.input_stream(stdout)
        #batch_count = 0
        #row_count = 0
        #result = {}
        #try:
        #    while True:
        #        reader = pyarrow.ipc.RecordBatchStreamReader(istream)
        #        try:
        #            while True:
        #                batch = reader.read_next_batch()
        #                batch_count += 1
        #                row_count += batch.num_rows
        #                result.get(batch.schema, []).append(batch)
        #        except StopIteration:
        #            batch_count = 0
        #            row_count = 0
        #except:
        #    logger.debug("completed all readers")
