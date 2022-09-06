import asyncio
import io
from collections import defaultdict

import pyarrow as pa
import vast.utils.arrow
import vast.utils.config
import vast.utils.logging

logger = vast.utils.logging.get(__name__)


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
    """An instance of a VAST node."""

    def __init__(self, config=vast.utils.config.parse()):
        self.config = config

    @staticmethod
    async def start(config=vast.utils.config.parse(), **kwargs):
        """Starts a VAST node."""
        self = VAST(config)
        proc = await CLI().start(**kwargs).exec()
        await proc.communicate()
        logger.debug(proc.stderr.decode())
        return self

    # TODO: agree on API.
    async def live_query(self, expression: str, callback):
        proc = await CLI().export(continuous=True).json(expression).exec()
        while True:
            if proc.stdout.at_eof():
                break
            line = await proc.stdout.readline()
            await callback(line)
        await proc.communicate()
        if proc.returncode != 0:
            logger.warning(f"vast exited with non-zero code: {proc.returncode}")

    async def export(self, expression: str, limit: int = 100):
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
