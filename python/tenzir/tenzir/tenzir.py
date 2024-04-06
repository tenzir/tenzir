import asyncio
import json
import secrets
import time
from abc import ABC
from enum import Enum, auto
from typing import AsyncIterable

import pyarrow as pa
import tenzir.utils.arrow
import tenzir.utils.config
import tenzir.utils.logging

from .cli import CLI

logger = tenzir.utils.logging.get(__name__)


class ExportMode(Enum):
    HISTORICAL = auto()
    CONTINUOUS = auto()
    UNIFIED = auto()


class TableSlice(ABC):
    """The Tenzir abstraction wrapping Arrow record batches"""


class PyArrowTableSlice(TableSlice):
    """A TableSlice internally represented by a PyArrow RecordBatch"""

    def __init__(self, batch: pa.RecordBatch):
        self._batch = batch


class _LazyReader:
    """Helper class that instantiates the PyArrow RecordBatchStreamReader in a
    dedicated thread to avoid blocking"""

    def __init__(self, source):
        self.source = source
        self._reader = None

    async def get(self):
        if self._reader is None:
            self._reader = await asyncio.to_thread(
                pa.RecordBatchStreamReader, self.source
            )
        return self._reader


class AsyncRecordBatchStreamReader:
    """A thread based wrapper that makes PyArrow RecordBatchStreamReader
    async"""

    def __init__(self, source):
        self.reader: _LazyReader = _LazyReader(source)

    def __aiter__(self):
        return self

    async def __anext__(self) -> TableSlice:
        reader = await self.reader.get()

        def _next_batch():
            try:
                return PyArrowTableSlice(reader.read_next_batch())
            except StopIteration:
                return None

        value = await asyncio.to_thread(_next_batch)
        if value is None:
            raise StopAsyncIteration
        return value


class Tenzir:
    """An instance of a Tenzir node."""

    def __init__(self, endpoint=None):
        cli_kwargs = {"endpoint": endpoint} if endpoint else {}
        self.cli = CLI(**cli_kwargs)

    @staticmethod
    def _export_args(mode: ExportMode, limit: int):
        args = {}
        if mode == ExportMode.CONTINUOUS:
            args["continuous"] = True
        elif mode == ExportMode.UNIFIED:
            args["unified"] = True
        if limit > 0:
            args["max_events"] = limit
        return args

    async def export(
        self,
        expression: str,
        mode: ExportMode = ExportMode.HISTORICAL,
        limit: int = 100,
    ) -> AsyncIterable[TableSlice]:
        """Executes a query and receives results as Arrow Tables."""
        if limit == 0:
            return
        cmd = self.cli.export(**Tenzir._export_args(mode, limit))
        if expression == "":
            cmd = cmd.arrow()
        else:
            cmd = cmd.arrow(expression)
        proc = cmd.sync_exec()

        def log():
            id = secrets.token_hex(6)
            export_str = f"export({expression}, {mode}, {limit})"
            logger.debug(f"[export-{id}] Start logging for {export_str}")
            for line in iter(proc.stderr.readline, b""):
                logger.debug(f"[export-{id}] {line.decode().strip()}")
            logger.debug(f"[export-{id}] Stop logging")

        # TODO: don't use default thread pool here as this method
        # will block one thread per running export, risking exhaustion of
        # the pool if we run many in parallel.
        t = asyncio.create_task(asyncio.to_thread(log))

        try:
            # Tenzir concatenates IPC streams for different types so we need to
            # spawn multiple stream readers
            while True:
                logger.debug("starting new record batch stream")
                async for batch in AsyncRecordBatchStreamReader(proc.stdout):
                    yield batch
        except Exception as e:
            if isinstance(e, pa.ArrowInvalid):
                logger.debug("completed processing stream of record batches")
            else:
                # Use SIGKILL as we are already in an unexpected error state, so
                # there is no point in trying a graceful exit.
                proc.kill()
                # We set a timeout to make sure that we don't hang while tearing
                # down the logging task. The duration of 3s is arbitrary and is
                # expected to be more than enough for the logger task to
                # complete.
                await asyncio.wait_for(t, 3)
                raise e

    async def status(self, timeout=0, retry_delay=0.5, **kwargs) -> dict:
        """Executes the `tenzir-ctl status` command and return the response string.

        If `timeout` is greater than 0, the invocation of `tenzir-ctl status` will be
        retried every `retry_delay` seconds for at most `timeout` seconds.

        Examples: `status()`, `status(timeout=30, detailed=True)`.
        """
        start = time.time()
        while True:
            proc = await self.cli.status(**kwargs).exec()
            stdout, stderr = await proc.communicate()
            logger.debug(stderr.decode())
            if proc.returncode == 0:
                result = json.loads(stdout.decode("utf-8"))
                assert isinstance(result, dict), "Argument of wrong type!"
                return result
            else:
                duration = time.time() - start
                if duration > timeout:
                    msg = f"tenzir-ctl status failed with code {proc.returncode}"
                    raise Exception(msg)
                await asyncio.sleep(retry_delay)
