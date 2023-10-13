import copy
import sys
import os
import types
import math
import pyarrow as pa
from typing import Dict, Tuple, Generator
from pytenzir.utils.arrow import make_record_batch, infer_type, extension_array
from pyarrow import RecordBatch
from collections.abc import MutableMapping
from collections import defaultdict

"""
NOTE: This script is intended to be used by and developed alongside
tenzir's `python` pipeline operator, and has little use outside
of that narrow use-case.
"""


def _log(*args):
    prefix = "debug: "
    z = " ".join(map(str, args))
    y = prefix + " " + z.replace("\n", "\n" + prefix + " ")
    print(y, file=sys.stderr)


def _flatten_struct_array(field, array) -> Generator:
    if isinstance(array, pa.StructArray):
        for x, y in zip(field.flatten(), array.flatten()):
            yield from _flatten_struct_array(x, y)
    else:
        yield field, array


def _flatten_batch(batch: pa.RecordBatch) -> Generator:
    for i, field in enumerate(batch.schema):
        array = batch.column(i)
        yield from _flatten_struct_array(field, array)


def _flatten_dict(d: dict, parent_key: str, original_dict: dict) -> Generator:
    for key, value in d.items():
        new_key = parent_key + "." + key
        if isinstance(value, dict):
            inner = original_dict.get(key, {})
            if not isinstance(inner, dict):
                inner = {}
            yield from _flatten_dict_generator(value, new_key, inner)
        else:
            touched = not (key in original_dict and id(original_dict[key]) == id(value))
            yield new_key, value, touched


def _find_first_nonnull(xs):
    for x in xs:
        if x is not None:
            return x
    return None


class _ResultsBuffer:
    """Holds the values produced by the user code per row, and finally assembles them into a new record batch with updated schema"""

    def __init__(self, original_batch: pa.RecordBatch):
        self.original_batch = original_batch
        self.input_values = original_batch.to_pydict()
        self.output_fields: Set[str] = set()
        self.output_values: Dict[str, list] = defaultdict(list)
        self.current_row = None
        # The `RecordBatch.to_pydict()` transformation loses some information (eg. enums are rendered as strings),
        # so we want to detect which values were actually modified by the user and use the original type information
        # for the rest.
        # For nested values, user code directly modifies the "original" dict,
        # so we can't detect changes unless we copy the original values.
        self.original_values = copy.deepcopy(self.input_values)

    def start_row(self, i, local_vars: dict):
        # Sanity check to ensure none of our own variables will leak into the output
        for k, v in local_vars.items():
            assert k.startswith("_")
        self.current_row = i
        for key, values in self.input_values.items():
            yield key, values[i]

    def finish_row(self, local_vars: dict):
        i = self.current_row
        assert i is not None
        new_locals = []
        for key, value in local_vars.items():
            # Ignore `import` statements
            if isinstance(value, types.ModuleType):
                continue
            new_locals.append(key)
            # Ignore all variables starting with an underscore.
            if key.startswith("_"):
                continue
            # Record the value of the variable as an output field.
            if isinstance(value, dict):
                for flat_key, value, was_touched in _flatten_dict(
                    value, key, self.original_values[key][i]
                ):
                    self.output_fields.add(flat_key)
                    if was_touched:
                        self.output_values[flat_key].append(value)
            else:
                self.output_fields.add(key)
                was_touched = key not in self.original_values or id(
                    self.original_values[key][i]
                ) != id(value)
                if was_touched:
                    self.output_values[key].append(value)
        # Manual scope cleanup, to prevent user-defined variables
        # from accidentally surviving into the next loop iteration.
        for key in new_locals:
            local_vars.pop(key, None)

    def finish(self) -> pa.RecordBatch:
        output_data: Dict[str, Tuple[pa.Field, pa.Array]] = {}
        # Construct flattened output batch
        for field, array in _flatten_batch(self.original_batch):
            if field.name in self.output_fields:
                output_data[field.name] = (field, array)
        # Overwrite the fields that were changed by the user code.
        for key, values in self.output_values.items():
            type_ = infer_type(_find_first_nonnull(values))
            field = pa.field(key, type_)
            array = extension_array(values, type_)
            output_data[key] = (field, array)
        # Construct final record batch.
        fields = [field for field, _ in output_data.values()]
        arrays = [array for _, array in output_data.values()]
        return RecordBatch.from_arrays(arrays=arrays, schema=pa.schema(fields))


def _execute_user_code(__batch: pa.RecordBatch, __code: str):
    __buffer = _ResultsBuffer(__batch)
    for __i in range(__batch.num_rows):
        for __key, __value in __buffer.start_row(__i, locals()):
            locals()[__key] = __value
        # == Run the user-provided code ===========
        exec(__code, globals(), locals())
        # =========================================
        __buffer.finish_row(locals())
    return __buffer.finish()


def main():
    codepipe = int(os.environ["TENZIR_PYTHON_OPERATOR_CODEFD"])
    code = os.read(codepipe, 128 * 1024)

    istream = pa.input_stream(sys.stdin.buffer)
    ostream = pa.output_stream(sys.stdout.buffer)

    while True:
        try:
            reader = pa.ipc.RecordBatchStreamReader(istream)
            batch_in = reader.read_next_batch()
            batch_out = _execute_user_code(batch_in, code)
            writer = pa.ipc.RecordBatchStreamWriter(ostream, batch_out.schema)
            writer.write(batch_out)
            sys.stdout.flush()
        except pa.lib.ArrowInvalid:
            break
