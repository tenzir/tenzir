from collections import defaultdict
from typing import Any, AsyncIterable
from dataclasses import dataclass

import pyarrow as pa
import vast.utils.arrow
import vast.utils.logging

from .vast import TableSlice

logger = vast.utils.logging.get("vast.vast")


def to_pyarrow(batch: TableSlice) -> pa.RecordBatch:
    """Represent the provided TableSlice as a PyArrow RecordBatch"""
    from .vast import PyArrowTableSlice

    if not isinstance(batch, PyArrowTableSlice):
        raise TypeError(f"Cannot convert {type(batch)} to pyarrow.RecordBatch")
    return batch._batch


async def collect_pyarrow(
    stream: AsyncIterable[TableSlice],
) -> dict[str, list[pa.Table]]:
    """Iterate through the TableSlice stream and sort the record batches by
    schema as lists of PyArrow Tables with identical schemas"""
    num_batches = 0
    num_rows = 0
    batches = defaultdict(list)
    async for slice in stream:
        batch = to_pyarrow(slice)
        name = vast.utils.arrow.name(batch.schema)
        logger.debug(f"got batch of {name}")
        num_batches += 1
        num_rows += batch.num_rows
        # TODO: this might be slow, but schemas are not
        # hashable. The string representation is the next best
        # thing to determine uniqueness.
        batches[batch.schema.to_string()].append(batch)
    result = defaultdict(list)
    for (_, batches) in batches.items():
        table = pa.Table.from_batches(batches)
        name = vast.utils.arrow.name(table.schema)
        result[name].append(table)
    return result


@dataclass
class VastRow:
    """A row wise representation of the data and metadata"""

    name: str
    data: dict[str, Any]


async def to_rows(
    stream: AsyncIterable[TableSlice],
) -> AsyncIterable[VastRow]:
    """Convert the table slice iterator to a row by row iterator"""
    async for slice in stream:
        batch = to_pyarrow(slice)
        name = vast.utils.arrow.name(batch.schema)
        for row in batch.to_pylist():
            yield VastRow(name, row)
