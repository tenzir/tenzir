import asyncio
import io
import json
import time
from collections import defaultdict
from enum import Enum, auto
from typing import Any, AsyncIterable, Dict

import pyarrow as pa
import vast.utils.arrow
import vast.utils.config
import vast.utils.logging

from .cli import CLI

logger = vast.utils.logging.get("vast.vast")


class ExportMode(Enum):
    HISTORICAL = auto()
    CONTINUOUS = auto()
    UNIFIED = auto()


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
    async def export_rows(
        expression: str, mode: ExportMode, limit: int = -1
    ) -> AsyncIterable[Dict[str, Any]]:
        """Get a row wise view of the data

        This method does not provide type information because it is using json
        as export format. It is a temporary workaround for asynchronicity issues
        with PyArrow, but we plan to have all VAST Python exports properly typed
        and backed by Arrow."""
        if limit == 0:
            return
        args = {}
        match mode:
            case ExportMode.CONTINUOUS:
                args["continuous"] = True
            case ExportMode.UNIFIED:
                args["unified"] = True
            case ExportMode.HISTORICAL:
                pass
        if limit > 0:
            args["max_events"] = limit
        proc = await CLI().export(**args).json(expression).exec()
        while True:
            if proc.stdout.at_eof():
                break
            line = await proc.stdout.readline()
            if line.strip() == b"":
                continue
            line_dict = json.loads(line)
            assert isinstance(line_dict, Dict)
            yield line_dict

        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            logger.error(stderr.decode())

    @staticmethod
    async def status(timeout=0, retry_delay=0.5, **kwargs) -> str:
        """Executes the `vast status` command and return the response string.

        If `timeout` is greater than 0, the invocation of `vast status` will be
        retried every `retry_delay` seconds for at most `timeout` seconds.

        Examples: `status()`, `status(timeout=30, detailed=True)`.
        """
        start = time.time()
        while True:
            proc = await CLI().status(**kwargs).exec()
            stdout, stderr = await proc.communicate()
            logger.debug(stderr.decode())
            if proc.returncode == 0:
                return stdout.decode("utf-8")
            else:
                duration = time.time() - start
                if duration > timeout:
                    msg = f"VAST status failed with code {proc.returncode}"
                    raise Exception(msg)
                await asyncio.sleep(retry_delay)

    @staticmethod
    async def count(*args, **kwargs) -> int:
        """
        Executes the VAST count command and return the response number.
        Examples: `count()`, `count("#type == /suricata.alert/", estimate=True)`.
        """
        proc = await CLI().count(*args, **kwargs).exec()
        stdout, stderr = await proc.communicate()
        logger.debug(stderr.decode())
        if proc.returncode != 0:
            msg = f"VAST count failed with code {proc.returncode}"
            raise Exception(msg)
        return int(stdout.decode("utf-8"))
