import io
from collections import defaultdict

import pyarrow as pa
import vast.utils.arrow
import vast.utils.config
import vast.utils.logging

from .cli import CLI

logger = vast.utils.logging.get("vast.vast")


class VAST:
    """An instance of a VAST node."""

    def __init__(self):
        pass

    @staticmethod
    async def start():
        """Starts a VAST node."""
        self = VAST()
        proc = await CLI().start().exec()
        await proc.communicate()
        logger.debug(proc.stderr.decode())
        return self

    # TODO: agree on API.
    @staticmethod
    async def export_continuous(expression: str, callback):
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

    @staticmethod
    async def status(**kwargs) -> str:
        """
        Executes the VAST status command and return the response string.
        Examples: `status()`, `status(detailed=True)`.
        """
        proc = await CLI().status(**kwargs).exec()
        stdout, stderr = await proc.communicate()
        logger.debug(stderr.decode())
        return stdout.decode("utf-8")

    @staticmethod
    async def count(*args, **kwargs) -> int:
        """
        Executes the VAST count command and return the response number.
        Examples: `count()`, `count("#type ~ /suricata.alert/", estimate=True)`.
        """
        proc = await CLI().count(*args, **kwargs).exec()
        stdout, stderr = await proc.communicate()
        logger.debug(stderr.decode())
        return int(stdout.decode("utf-8"))
