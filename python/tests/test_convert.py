import numpy as np
import pyarrow as pa
import pytest

import tenzir.utils.arrow as vua
from tenzir.tenzir.convert import arrow_dict_to_json_dict

new_array = vua.extension_array


def native_types_batch() -> pa.RecordBatch:
    """Create single rowed RecordBatch with all native Arrow types"""
    arrays = []
    arrays.append(new_array([None], pa.null()))
    arrays.append(new_array([True], pa.bool_()))
    arrays.append(new_array([1], pa.int8()))
    arrays.append(new_array([2], pa.int16()))
    arrays.append(new_array([3], pa.int32()))
    arrays.append(new_array([5], pa.int64()))
    arrays.append(new_array([6], pa.uint8()))
    arrays.append(new_array([7], pa.uint16()))
    arrays.append(new_array([8], pa.uint32()))
    arrays.append(new_array([9], pa.uint64()))
    arrays.append(new_array([np.float16(10.0)], pa.float16()))
    arrays.append(new_array([np.float32(11.1)], pa.float32()))
    arrays.append(new_array([np.float64(11.2)], pa.float64()))
    arrays.append(new_array([10000], pa.time32("s")))
    arrays.append(new_array([9000000], pa.time64("ns")))
    arrays.append(new_array([9000000], pa.timestamp("us")))
    arrays.append(new_array([3600 * 24 * 3], pa.date32()))
    arrays.append(new_array([1000 * 3600 * 24 * 3], pa.date64()))
    arrays.append(new_array([10], pa.duration("s")))
    # arrays.append(new_array([?], pa.month_day_nano_interval()))
    arrays.append(new_array([b"000"], pa.binary(3)))
    arrays.append(new_array(["hello"], pa.string()))
    arrays.append(new_array([b"000"], pa.large_binary()))
    arrays.append(new_array(["hello"], pa.large_string()))
    arrays.append(new_array([8], pa.decimal128(16)))
    arrays.append(new_array([["hello", "list"]], pa.list_(pa.string())))
    arrays.append(new_array([["hello", "largelist"]], pa.large_list(pa.string())))
    arrays.append(new_array([[("hello", True)]], pa.map_(pa.string(), pa.bool_())))
    fields = [("f1", pa.int32()), ("f2", pa.string())]
    arrays.append(new_array([{"f1": 10, "f2": "hello"}], pa.struct(fields)))

    for array in arrays:
        array.validate(full=True)

    col_names = [arr.__class__.__name__ for arr in arrays]
    return pa.RecordBatch.from_arrays(arrays, names=col_names)


def extension_types_batch() -> pa.RecordBatch:
    """Create single rowed RecordBatch with all Tenzir extension types"""
    arrays = []
    arrays.append(new_array(["10.1.21.165"], vua.IPType()))
    arrays.append(new_array(["10.1.20.0/25"], vua.SubnetType()))
    fields = {"foo": 1, "bar": 2, "baz": 4}
    arrays.append(new_array(["foo"], vua.EnumType(fields)))

    for array in arrays:
        array.validate()

    col_names = [arr.type.extension_name for arr in arrays]
    return pa.RecordBatch.from_arrays(arrays, names=col_names)


@pytest.mark.asyncio
async def test_arrow_dict_to_json_dict_native_types():
    native_dict = native_types_batch().to_pylist()[0]

    assert arrow_dict_to_json_dict(native_dict) == {
        "NullArray": None,
        "BooleanArray": True,
        "Int8Array": 1,
        "Int16Array": 2,
        "Int32Array": 3,
        "Int64Array": 5,
        "UInt8Array": 6,
        "UInt16Array": 7,
        "UInt32Array": 8,
        "UInt64Array": 9,
        "HalfFloatArray": 10.0,
        "FloatArray": 11.100000381469727,
        "DoubleArray": 11.2,
        "Time32Array": "02:46:40",
        "Time64Array": "00:00:00.009000",
        "TimestampArray": "1970-01-01T00:00:09",
        "Date32Array": "2679-09-01",
        "Date64Array": "1970-01-04",
        "DurationArray": "0:00:10",
        "FixedSizeBinaryArray": "MDAw",
        "StringArray": "hello",
        "LargeBinaryArray": "MDAw",
        "LargeStringArray": "hello",
        "Decimal128Array": 8,
        "ListArray": ["hello", "list"],
        "LargeListArray": ["hello", "largelist"],
        "MapArray": [("hello", True)],
        "StructArray": {"f1": 10, "f2": "hello"},
    }


@pytest.mark.asyncio
async def test_arrow_dict_to_json_dict_extension_types():
    extension_dict = extension_types_batch().to_pylist()[0]

    assert arrow_dict_to_json_dict(extension_dict) == {
        "tenzir.ip": "10.1.21.165",
        "tenzir.subnet": "10.1.20.0/25",
        "tenzir.enumeration": "foo",
    }
