import asyncio
import io
from collections import defaultdict

from dynaconf import Dynaconf
import pyarrow as pa
import stix2

from .fabric import Fabric
import vast.backbones
import vast.bridges.stix
import vast.utils.arrow
import vast.utils.logging

logger = vast.utils.logging.get("node")


class CLI:
    """A commmand-line wrapper for the VAST executable."""

    @staticmethod
    def arguments(**kwargs):
        result = []
        for k, v in kwargs.items():
            if v is True:
                result.append(f"--{k.replace('_','-')}")
            else:
                result.append(f"--{k.replace('_','-')}={v}")
        return result

    def __init__(self, **kwargs):
        self.args = CLI.arguments(**kwargs)

    async def exec(self, stdin=False):
        async def run(*args, stdin):
            return await asyncio.create_subprocess_exec(
                "vast",
                *args,
                stdin=asyncio.subprocess.PIPE if stdin else None,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

        logger.debug(f"> vast {' '.join(self.args)}")
        if not stdin:
            return await run(*self.args, stdin=False)
        proc = await run(*self.args, stdin=True)
        # TODO: for large inputs, the buffering may be excessive here. We may
        # have to switch to a streaming approach.
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


class VAST:
    """An instance of a VAST node that sits between an application and the
    fabric."""

    @staticmethod
    async def create(config: Dynaconf):
        backbone = vast.backbones.InMemory()
        fabric = Fabric(config, backbone)
        self = VAST(config, fabric)
        logger.debug("subscribing to STIX topics")
        await self.fabric.pull(self._on_stix_indicator)
        return self

    def __init__(self, config: Dynaconf, fabric: Fabric):
        self.config = config
        self.fabric = fabric
        self.stix_bridge = vast.bridges.stix.STIX()

    async def start(self, **kwargs):
        """Starts a VAST node."""
        proc = await CLI(**kwargs).start().exec()
        await proc.communicate()
        logger.debug(proc.stderr.decode())
        # TODO: Start a matcher
        # cmd = CLI(plugins="matcher").matcher().start(match_types="[:string]").test()
        return proc

    async def republish(self, type: str):
        """Subscribes to an event type on ingest path and publishes the events
        directly to the fabric."""
        callback = lambda line: self.fabric.backbone.publish(type, line)
        await self._live_query(f'#type == "{type}"', callback)

    # Translates an STIX Indicator into a VAST query and publishes the results
    # as STIX Bundle.
    async def _on_stix_indicator(self, indicator: stix2.Indicator):
        logger.debug(f"got {indicator}")
        if indicator.pattern_type == "vast":
            logger.debug(f"got VAST expression {indicator.pattern}")
        elif indicator.pattern_type == "sigma":
            logger.debug(f"got Sigma rule {indicator.pattern}")
        else:
            logger.warn(f"got unsupported pattern type {indicator.pattern_type}")
            return
        # Run the query.
        results = await self._query(indicator.pattern)
        if len(results) == 0:
            logger.info(f"got no results for query: {indicator.pattern}")
            return
        # TODO: go beyond Zeek connection events.
        zeek_conn = results["zeek.conn"]
        if not zeek_conn:
            logger.warning("no connection events found")
            return
        bundle = self.stix_bridge.make_sighting_from_flows(indicator, zeek_conn)
        await self.fabric.push(bundle)

    async def _live_query(self, expression: str, callback):
        proc = await CLI().export(continuous=True).json(expression).exec()
        while True:
            if proc.stdout.at_eof():
                break
            line = await proc.stdout.readline()
            await callback(line)
        await proc.communicate()
        if proc.returncode != 0:
            logger.warning(f"vast exited with non-zero code: {proc.returncode}")

    async def _query(self, expression: str, limit: int = 100):
        """Executes a VAST and receives results as Arrow Tables."""
        proc = await CLI().export(max_events=limit).arrow(expression).exec()
        stdout, stderr = await proc.communicate()
        logger.debug(stderr.decode())
        istream = pa.input_stream(io.BytesIO(stdout))
        num_batches = 0
        num_rows = 0
        batches = defaultdict(list)
        try:
            while True:
                reader = pa.ipc.RecordBatchStreamReader(istream)
                try:
                    while True:
                        batch = reader.read_next_batch()
                        name = vast.utils.arrow.name(batch.schema)
                        logger.debug(f"got batch of {name}")
                        num_batches += 1
                        num_rows += batch.num_rows
                        # TODO: this might be slow, but schemas are not
                        # hashable. The string representation is the next best
                        # thing to determine uniqueness.
                        batches[batch.schema.to_string()].append(batch)
                except StopIteration:
                    logger.debug(f"read {num_batches}/{num_rows} batches/rows")
                    num_batches = 0
                    num_rows = 0
        except pa.ArrowInvalid:
            logger.debug("completed processing stream of record batches")
        result = defaultdict(list)
        for (_, batches) in batches.items():
            table = pa.Table.from_batches(batches)
            name = vast.utils.arrow.name(table.schema)
            result[name].append(table)
        return result
