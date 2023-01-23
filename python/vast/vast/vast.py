import asyncio
import time
from abc import ABC
from enum import Enum, auto
from typing import AsyncIterable, Optional, AsyncGenerator

import pyarrow as pa
import vast.utils.arrow
import vast.utils.config
import vast.utils.logging

from .cli import CLI

logger = vast.utils.logging.get(__name__)


class ExportMode(Enum):
    HISTORICAL = auto()
    CONTINUOUS = auto()
    UNIFIED = auto()


class TableSlice(ABC):
    """The VAST abstraction wrapping Arrow record batches"""


class PyArrowTableSlice(TableSlice):
    """A TableSlice internally represented by a PyArrow RecordBatch"""

    def __init__(self, batch: pa.RecordBatch):
        self._batch = batch


class AsyncRecordBatchStreamReader:
    def __init__(self, source):
        self.reader = pa.RecordBatchStreamReader(source)
        self.loop = asyncio.get_event_loop()

    def _next_batch(self) -> Optional[TableSlice]:
        try:
            return PyArrowTableSlice(self.reader.read_next_batch())
        except StopIteration:
            return None

    def __aiter__(self):
        return self

    async def __anext__(self) -> TableSlice:
        value = await self.loop.run_in_executor(None, self._next_batch)
        if value is None:
            raise StopAsyncIteration
        return value


class VAST:
    """An instance of a VAST node."""

    def __init__(self):
        pass

    @staticmethod
    def _export_args(mode: ExportMode, limit: int):
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
        return args

    @staticmethod
    async def start():
        """Starts a VAST node."""
        self = VAST()
        proc = await CLI().start().exec()
        await proc.communicate()
        logger.debug(proc.stderr.decode())
        return self

    @staticmethod
    async def export(
        expression: str, mode: ExportMode = ExportMode.HISTORICAL, limit: int = 100
    ) -> AsyncIterable[TableSlice]:
        """Executes a VAST and receives results as Arrow Tables."""
        if limit == 0:
            return
        cmd = CLI().export(**VAST._export_args(mode, limit))
        if expression == "":
            cmd = cmd.arrow()
        else:
            cmd = cmd.arrow(expression)
        proc = cmd.sync_exec()
        try:
            # VAST concatenates IPC streams for different types so we need to
            # spawn multiple stream readers
            while True:
                async for batch in AsyncRecordBatchStreamReader(proc.stdout):
                    yield batch
        except Exception as e:
            # Process should be completed when stdout is closed, but Popen state
            # might not be updated yet. Calling `wait()` with a very short
            # timeout ensures the returncode is properly set.
            if isinstance(e, pa.ArrowInvalid) and proc.wait(0.1) == 0:
                logger.debug("completed processing stream of record batches")
            else:
                logger.error(proc.stderr)
                raise e

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
