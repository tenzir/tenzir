import base64
from collections import defaultdict
from dataclasses import dataclass
import datetime
from decimal import Decimal
from typing import Any, AsyncIterable

import numpy as np
import pyarrow as pa
import tenzir.utils.arrow
import tenzir.utils.logging

from .tenzir import TableSlice

logger = tenzir.utils.logging.get("tenzir.tenzir")

_JSON_COMPATIBILITY_DOCSTRING_ = """JSON types are numbers, booleans, strings, arrays and objects
- dates and times are formated with ISO 8601
- raw bytes are Base64 encoded"""


def to_pyarrow(batch: TableSlice) -> pa.RecordBatch:
    """Represent the provided TableSlice as a PyArrow RecordBatch"""
    from .tenzir import PyArrowTableSlice

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
        name = tenzir.utils.arrow.name(batch.schema)
        logger.debug(f"got batch of {name}")
        num_batches += 1
        num_rows += batch.num_rows
        # TODO: this might be slow, but schemas are not
        # hashable. The string representation is the next best
        # thing to determine uniqueness.
        batches[batch.schema.to_string()].append(batch)
    result = defaultdict(list)
    for _, batches in batches.items():
        table = pa.Table.from_batches(batches)
        name = tenzir.utils.arrow.name(table.schema)
        result[name].append(table)
    return result


@dataclass
class VastRow:
    """A row wise representation of the data and metadata

    - name: the Tenzir type for the row
    - data: the event data contained in the row"""

    name: str
    data: dict[str, Any]


def arrow_dict_to_json_dict(dictionary):
    f"""Convert a result item of PyArrow to_pylist into a dictionary with
    JSON compatible value types

    {_JSON_COMPATIBILITY_DOCSTRING_}"""
    for key, value in dictionary.items():
        if isinstance(value, dict):
            arrow_dict_to_json_dict(value)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, (dict, list)):
                    arrow_dict_to_json_dict(item)
                elif not isinstance(item, (int, float, bool)):
                    item = str(item)
        elif isinstance(value, (np.floating, Decimal)):
            dictionary[key] = float(value)
        elif isinstance(value, bytes):
            dictionary[key] = base64.b64encode(value).decode()
        elif isinstance(value, (datetime.time, datetime.datetime, datetime.date)):
            dictionary[key] = value.isoformat()
        elif value is None or isinstance(value, (int, float, bool)):
            dictionary[key] = value
        elif not isinstance(value, (int, float, bool)):
            dictionary[key] = str(value)
    return dictionary


async def to_json_rows(
    stream: AsyncIterable[TableSlice],
) -> AsyncIterable[VastRow]:
    f"""Convert the TableSlice iterator to a row by row iterator with value types
    dumbed down to JSON compatible types

    {_JSON_COMPATIBILITY_DOCSTRING_}"""
    async for slice in stream:
        batch = to_pyarrow(slice)
        name = tenzir.utils.arrow.name(batch.schema)
        for row in batch.to_pylist():
            yield VastRow(name, arrow_dict_to_json_dict(row))
