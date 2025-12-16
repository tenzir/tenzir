from __future__ import annotations

import asyncio
import os
import shutil
import subprocess
from collections.abc import AsyncIterable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Any, Optional, Union

import pyarrow as pa
import tenzir_common.logging as core_logging

logger = core_logging.get(__name__)

# ---------------------------------------------------------------------------
# Table slice wrappers and IPC helpers


class TableSlice:
    """Marker base class for table slice wrappers."""


class PyArrowTableSlice(TableSlice):
    """A TableSlice backed by a PyArrow RecordBatch."""

    def __init__(self, batch: pa.RecordBatch) -> None:
        self._batch = batch


class _LazyReader:
    """Instantiate a RecordBatchStreamReader in a thread on first access."""

    def __init__(self, source) -> None:
        self._source = source
        self._reader: Optional[pa.RecordBatchReader] = None

    async def get(self) -> pa.RecordBatchReader:
        if self._reader is None:
            self._reader = await asyncio.to_thread(pa.ipc.open_stream, self._source)
        return self._reader


class AsyncRecordBatchStreamReader(AsyncIterable[PyArrowTableSlice]):
    """Async wrapper around PyArrow's RecordBatchStreamReader."""

    def __init__(self, source) -> None:
        self._lazy_reader = _LazyReader(source)

    def __aiter__(self) -> AsyncRecordBatchStreamReader:
        return self

    async def __anext__(self) -> PyArrowTableSlice:
        reader = await self._lazy_reader.get()

        def _next_batch() -> Optional[pa.RecordBatch]:
            try:
                return reader.read_next_batch()
            except StopIteration:
                return None

        batch = await asyncio.to_thread(_next_batch)
        if batch is None:
            raise StopAsyncIteration
        return PyArrowTableSlice(batch)


# ---------------------------------------------------------------------------
# Pipeline configuration


@dataclass(frozen=True)
class PipelineSpec:
    """Description of a pipeline to execute."""

    text: str
    description: Optional[str] = None

    def __str__(self) -> str:
        return self.text

    @classmethod
    def from_file(cls, path: Union[str, os.PathLike[str]]) -> PipelineSpec:
        file_path = Path(path)
        return cls(file_path.read_text(), description=str(file_path))

    @classmethod
    def from_stages(cls, stages: Sequence[str]) -> PipelineSpec:
        return cls(" | ".join(stages), description=" | ".join(stages))


@dataclass(frozen=True)
class PipelineOptions:
    """Additional execution options for a pipeline run."""

    extra_args: Sequence[str] = ()
    env: Optional[Mapping[str, str]] = None
    cwd: Optional[Union[str, os.PathLike[str]]] = None
    timeout: Optional[float] = None


class PipelineError(RuntimeError):
    """Raised when a pipeline exits unsuccessfully."""

    def __init__(
        self,
        message: str,
        *,
        returncode: int,
        stderr: Optional[str],
        command: Sequence[str],
    ) -> None:
        super().__init__(message)
        self.returncode = returncode
        self.stderr = stderr
        self.command = tuple(command)


# ---------------------------------------------------------------------------
# IO adapters


class _InputAdapter:
    async def feed(self, process: subprocess.Popen[Any]) -> None:
        raise NotImplementedError


def _resolve_tenzir_binary() -> Path:
    pkg_bin_dir = Path(__file__).resolve().parent.parent / "bin"
    candidate = pkg_bin_dir / "tenzir"
    if candidate.is_file() and os.access(candidate, os.X_OK):
        return candidate
    fallback = shutil.which("tenzir")
    if fallback:
        return Path(fallback)
    msg = "failed to resolve bundled 'tenzir' executable"
    raise FileNotFoundError(msg)


async def _iter_record_batches(source) -> list[pa.RecordBatch]:
    batches: list[pa.RecordBatch] = []

    async def _handle(value) -> None:
        if isinstance(value, pa.RecordBatch):
            batches.append(value)
        elif isinstance(value, pa.Table):
            batches.extend(value.to_batches())
        elif isinstance(value, Iterable):
            for item in value:
                await _handle(item)
        elif hasattr(value, "__aiter__"):
            async for item in value:
                await _handle(item)
        else:
            msg = f"Unsupported Arrow input type: {type(value)!r}"
            raise TypeError(msg)

    await _handle(source)
    return batches


class _ArrowInputAdapter(_InputAdapter):
    def __init__(self, source) -> None:
        self._source = source

    async def feed(self, process: subprocess.Popen[Any]) -> None:
        stdin = process.stdin
        if stdin is None:
            msg = "pipeline does not expose stdin but Arrow input was configured"
            raise PipelineError(msg, returncode=-1, stderr=None, command=process.args)  # type: ignore[arg-type]
        batches = await _iter_record_batches(self._source)

        def _write() -> None:
            writer: Optional[pa.RecordBatchStreamWriter] = None
            try:
                for batch in batches:
                    if writer is None:
                        writer = pa.ipc.new_stream(stdin, batch.schema)
                    writer.write_batch(batch)
            finally:
                if writer is not None:
                    writer.close()
                stdin.close()

        await asyncio.to_thread(_write)


class _BytesInputAdapter(_InputAdapter):
    def __init__(self, data: Union[str, bytes]) -> None:
        self._data = data.encode() if isinstance(data, str) else data

    async def feed(self, process: subprocess.Popen[Any]) -> None:
        stdin = process.stdin
        if stdin is None:
            msg = "pipeline does not expose stdin but byte input was configured"
            raise PipelineError(msg, returncode=-1, stderr=None, command=process.args)  # type: ignore[arg-type]

        def _write() -> None:
            try:
                stdin.write(self._data)
            finally:
                stdin.close()

        await asyncio.to_thread(_write)


class _OutputHandle:
    async def collect(self) -> Any:
        raise NotImplementedError

    async def close(self) -> None:
        return None


class _ArrowOutputHandle(_OutputHandle):
    def __init__(self, stdout) -> None:
        self._reader = AsyncRecordBatchStreamReader(stdout)

    @property
    def stream(self) -> AsyncRecordBatchStreamReader:
        return self._reader

    async def collect(self) -> list[PyArrowTableSlice]:
        results: list[PyArrowTableSlice] = []
        async for batch in self._reader:
            results.append(batch)
        return results


class _JsonOutputHandle(_OutputHandle):
    def __init__(self, arrow_handle: _ArrowOutputHandle) -> None:
        self._arrow_handle = arrow_handle
        self._iterator: Optional[AsyncIterable[Any]] = None

    def _ensure_iterator(self) -> AsyncIterable[Any]:
        if self._iterator is None:
            from tenzir.tenzir import convert as _convert

            self._iterator = _convert.to_json_rows(self._arrow_handle.stream)
        return self._iterator

    @property
    def stream(self) -> AsyncIterable[Any]:
        return self._ensure_iterator()

    async def collect(self) -> list[Any]:
        results: list[Any] = []
        async for item in self._ensure_iterator():
            results.append(item)
        return results


class _BytesOutputHandle(_OutputHandle):
    def __init__(self, stdout) -> None:
        self._stdout = stdout
        self._buffer: Optional[bytes] = None
        self._stream = _BytesStream(self._stdout)

    @property
    def stream(self) -> _BytesStream:
        return self._stream

    async def collect(self) -> bytes:
        if self._buffer is None:
            self._buffer = await asyncio.to_thread(self._stdout.read)
        return self._buffer


class _BytesStream(AsyncIterable[bytes]):
    def __init__(self, stdout) -> None:
        self._stdout = stdout

    def __aiter__(self) -> _BytesStream:
        return self

    async def __anext__(self) -> bytes:
        chunk = await asyncio.to_thread(self._stdout.read1, 65536)
        if chunk == b"":
            raise StopAsyncIteration
        return chunk


class _CaptureStderrHandle:
    def __init__(self, stderr) -> None:
        self._stderr = stderr
        self._task = asyncio.create_task(asyncio.to_thread(self._stderr.read))
        self._cache: Optional[str] = None

    async def read(self) -> str:
        if self._cache is None:
            data = await self._task
            self._cache = data.decode("utf-8", errors="replace") if data else ""
        return self._cache

    async def close(self) -> None:
        await self.read()


class _OutputAdapter:
    def attach(self, process: subprocess.Popen[Any]) -> _OutputHandle:
        raise NotImplementedError


class _ArrowOutputAdapter(_OutputAdapter):
    def attach(self, process: subprocess.Popen[Any]) -> _ArrowOutputHandle:
        stdout = process.stdout
        if stdout is None:
            msg = "pipeline stdout is not available for arrow output"
            raise PipelineError(msg, returncode=-1, stderr=None, command=process.args)  # type: ignore[arg-type]
        return _ArrowOutputHandle(stdout)


class _JsonOutputAdapter(_OutputAdapter):
    def attach(self, process: subprocess.Popen[Any]) -> _JsonOutputHandle:
        return _JsonOutputHandle(_ArrowOutputAdapter().attach(process))


class _BytesOutputAdapter(_OutputAdapter):
    def attach(self, process: subprocess.Popen[Any]) -> _BytesOutputHandle:
        stdout = process.stdout
        if stdout is None:
            msg = "pipeline stdout is not available for byte output"
            raise PipelineError(msg, returncode=-1, stderr=None, command=process.args)  # type: ignore[arg-type]
        return _BytesOutputHandle(stdout)


class _StderrAdapter:
    def attach(self, process: subprocess.Popen[Any]) -> Optional[_CaptureStderrHandle]:
        raise NotImplementedError


class _CaptureStderrAdapter(_StderrAdapter):
    def attach(self, process: subprocess.Popen[Any]) -> _CaptureStderrHandle:
        stderr = process.stderr
        if stderr is None:
            msg = "pipeline stderr is not available for capture"
            raise PipelineError(msg, returncode=-1, stderr=None, command=process.args)  # type: ignore[arg-type]
        return _CaptureStderrHandle(stderr)


class PipelineIO:
    """Describe stdin/stdout/stderr handling for a pipeline run."""

    def __init__(
        self,
        *,
        input: Optional[_InputAdapter] = None,
        output: Optional[_OutputAdapter] = None,
        stderr: Optional[_StderrAdapter] = None,
    ) -> None:
        self.input = input
        self.output = output or PipelineIO.arrow_output()
        self.stderr = stderr or PipelineIO.capture_stderr()

    @classmethod
    def arrow(
        cls,
        *,
        input: Optional[_InputAdapter] = None,
        stderr: Optional[_StderrAdapter] = None,
    ) -> PipelineIO:
        return cls(input=input, output=cls.arrow_output(), stderr=stderr)

    @staticmethod
    def arrow_input(source) -> _ArrowInputAdapter:
        return _ArrowInputAdapter(source)

    @staticmethod
    def bytes_input(data: Union[str, bytes]) -> _BytesInputAdapter:
        return _BytesInputAdapter(data)

    @staticmethod
    def arrow_output() -> _ArrowOutputAdapter:
        return _ArrowOutputAdapter()

    @staticmethod
    def json_output() -> _JsonOutputAdapter:
        return _JsonOutputAdapter()

    @staticmethod
    def bytes_output() -> _BytesOutputAdapter:
        return _BytesOutputAdapter()

    @staticmethod
    def capture_stderr() -> _CaptureStderrAdapter:
        return _CaptureStderrAdapter()


# ---------------------------------------------------------------------------
# Pipeline execution


class PipelineRun:
    """Represents an in-flight pipeline execution."""

    def __init__(
        self,
        process: subprocess.Popen[Any],
        command: Sequence[str],
        output_handle: _OutputHandle,
        stderr_handle: Optional[_CaptureStderrHandle],
        input_task: Optional[asyncio.Task[None]],
    ) -> None:
        self._process = process
        self.command = tuple(command)
        self._output_handle = output_handle
        self._stderr_handle = stderr_handle
        self._input_task = input_task
        self._wait_result: Optional[int] = None
        self.output = output_handle.stream  # type: ignore[assignment]
        self.stderr = stderr_handle

    async def __aenter__(self) -> PipelineRun:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> Optional[bool]:
        try:
            rc = await self.wait()
            if exc_type is None and rc != 0:
                stderr_text = await self.collect_stderr()
                message = f"tenzir pipeline exited with code {rc}"
                raise PipelineError(
                    message,
                    returncode=rc,
                    stderr=stderr_text,
                    command=self.command,
                ) from None
            return None
        finally:
            await self._finalize()

    async def wait(self) -> int:
        if self._wait_result is None:
            self._wait_result = await asyncio.to_thread(self._process.wait)
            if self._input_task:
                await self._input_task
        return self._wait_result

    async def collect_output(self) -> Any:
        return await self._output_handle.collect()

    async def collect_stderr(self) -> Optional[str]:
        if not self._stderr_handle:
            return None
        return await self._stderr_handle.read()

    async def terminate(self) -> None:
        self._process.terminate()
        await self.wait()

    async def kill(self) -> None:
        self._process.kill()
        await self.wait()

    async def _finalize(self) -> None:
        try:
            if self._stderr_handle:
                await self._stderr_handle.close()
        finally:
            stdout = self._process.stdout
            if stdout is not None:
                stdout.close()
            stderr = self._process.stderr
            if stderr is not None:
                stderr.close()
            stdin = self._process.stdin
            if stdin is not None and not stdin.closed:
                stdin.close()


async def stream_pipeline(
    spec: Union[str, PipelineSpec],
    *,
    io: Optional[PipelineIO] = None,
    options: Optional[PipelineOptions] = None,
) -> PipelineRun:
    """Execute a pipeline and expose streaming access to stdout."""
    resolved_spec = spec if isinstance(spec, PipelineSpec) else PipelineSpec(str(spec))
    io = io or PipelineIO()
    options = options or PipelineOptions()

    binary = _resolve_tenzir_binary()
    command: list[str] = [
        binary.as_posix(),
        *map(str, options.extra_args or ()),
        str(resolved_spec),
    ]

    env = os.environ.copy()
    if options.env:
        env.update(options.env)

    stdin = subprocess.PIPE if io.input is not None else None
    stdout = subprocess.PIPE
    stderr = subprocess.PIPE if io.stderr is not None else None

    process = subprocess.Popen(
        command,
        stdin=stdin,
        stdout=stdout,
        stderr=stderr,
        cwd=options.cwd,
        env=env,
        text=False,
    )

    input_task = (
        asyncio.create_task(io.input.feed(process)) if io.input is not None else None
    )
    output_handle = io.output.attach(process)
    stderr_handle = io.stderr.attach(process) if io.stderr is not None else None

    return PipelineRun(
        process=process,
        command=command,
        output_handle=output_handle,
        stderr_handle=stderr_handle,
        input_task=input_task,
    )


@dataclass
class CompletedPipeline:
    output: Any
    stderr: Optional[str]
    returncode: int


async def run_pipeline(
    spec: Union[str, PipelineSpec],
    *,
    io: Optional[PipelineIO] = None,
    options: Optional[PipelineOptions] = None,
) -> CompletedPipeline:
    """Execute a pipeline and collect stdout/stderr before returning."""
    async with await stream_pipeline(spec, io=io, options=options) as run:
        output = await run.collect_output()
        stderr = await run.collect_stderr()
        returncode = await run.wait()
    return CompletedPipeline(output=output, stderr=stderr, returncode=returncode)


def run_pipeline_sync(
    spec: Union[str, PipelineSpec],
    *,
    io: Optional[PipelineIO] = None,
    options: Optional[PipelineOptions] = None,
) -> CompletedPipeline:
    """Synchronous helper for running a pipeline end-to-end."""
    return asyncio.run(run_pipeline(spec, io=io, options=options))
