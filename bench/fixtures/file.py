"""File-based fixtures for repo-local benchmarks."""

from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass
from pathlib import Path

from tenzir_bench.fixtures import (
    FixtureHandle,
    current_context,
    current_options,
    fixture,
)

_LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class RepeatInputOptions:
    """Structured configuration for the ``repeat_input`` benchmark fixture."""

    input_file: str | None = None
    repetitions: int = 1


def _resolve_path(benchmark_path: Path, raw: str) -> Path:
    value = raw.strip()
    if not value:
        raise ValueError("'repeat_input.input_file' must be a non-empty string")
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = (benchmark_path.parent / path).resolve()
    else:
        path = path.resolve()
    return path


def _materialize_repeated_input(
    *,
    source: Path,
    destination: Path,
    repetitions: int,
) -> Path:
    if repetitions <= 0:
        raise ValueError("'repeat_input.repetitions' must be a positive integer")
    destination.parent.mkdir(parents=True, exist_ok=True)
    _LOG.info(
        "Materializing repeated benchmark input %s from %s repeated %sx",
        destination,
        source,
        repetitions,
    )
    with destination.open("wb") as output:
        for _ in range(repetitions):
            with source.open("rb") as handle:
                shutil.copyfileobj(handle, output)
    return destination


@fixture(name="repeat_input", replace=True, options=RepeatInputOptions)
def repeat_input() -> FixtureHandle:
    """Override ``BENCHMARK_INPUT_PATH`` with a repeated copy of the dataset."""

    context = current_context()
    if context is None:
        raise RuntimeError("repeat_input fixture requires an active benchmark context")
    options = current_options("repeat_input")
    if not isinstance(options, RepeatInputOptions):
        raise ValueError("invalid options for fixture 'repeat_input'")

    input_path = context.dataset_path
    if options.input_file is not None:
        input_path = _resolve_path(context.definition.path, options.input_file)
        if not input_path.exists():
            raise RuntimeError(f"repeat_input input file does not exist: {input_path}")

    repeated_path = (
        context.output_root
        / "inputs"
        / f"{input_path.stem}-x{options.repetitions}{input_path.suffix}"
    )
    _materialize_repeated_input(
        source=input_path,
        destination=repeated_path,
        repetitions=options.repetitions,
    )
    return FixtureHandle(env={"BENCHMARK_INPUT_PATH": str(repeated_path)})
