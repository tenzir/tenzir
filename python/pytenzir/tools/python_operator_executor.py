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
from box import Box

"""
NOTE: This script is used by and developed alongside the built-in `python` 
pipeline operator in libtenzir, and has little use outside of that narrow use-case.
"""


def _log(*args):
    prefix = "debug: "
    z = " ".join(map(str, args))
    y = prefix + " " + z.replace("\n", "\n" + prefix + " ")
    print(y, file=sys.stderr)


class _ValueWrapper(object):
    """
    A transparent wrapper around some object. It serves as a marker so that
    we see an object that's *not* of type `_ValueWrapper` in the output we
    can assume it was created by the user.
    """
    def __init__(self, fieldname: str, obj):
        self._fieldname = fieldname
        self._wrapped_obj = obj

    def __getattr__(self, attr):
        return getattr(self._wrapped_obj, attr)

    def __len__(self):
        return len(self._wrapped_obj)

    @staticmethod
    def wrap_recursive(fieldname, obj):
        if isinstance(obj, dict):
            items = ( (key, _ValueWrapper.wrap_recursive(fieldname + "." + key, value)) for key, value in obj.items() )
            # Use `Box` to allow dot-notation for value access in user code.
            return Box(items)
        else:
            # TODO: Do we also need to special-case lists here?
            return _ValueWrapper(fieldname, obj)


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
            yield from _flatten_dict(value, new_key, inner)
        else:
            touched = not (key in original_dict and id(original_dict[key]) == id(value))
            yield new_key, value, touched


def _flatten_box(parent_key: str, dw: Box) -> Generator:
    for key, value in dw.items():
        new_key = parent_key + "." + key
        if isinstance(value, Box):
            yield from _flatten_box(new_key, value)
        elif isinstance(value, _ValueWrapper):
            changed = value._fieldname != new_key
            yield new_key, value._wrapped_obj, changed
        else:
            yield new_key, value, True


def _find_first_nonnull(xs):
    for x in xs:
        if x is not None:
            return x
    return None


class _ResultsBuffer:
    """
    Holds the values produced by the user code for each row, and finally assembles
    them into a new record batch with a flattened schema.
    """

    def __init__(self, original_batch: pa.RecordBatch):
        self.original_batch = original_batch
        self.input_values = original_batch.to_pydict()
        self.output_values: Dict[str, list] = defaultdict(list)
        self.current_row = None
        self.changed = set()

    def start_row(self, i, local_vars: dict):
        # Sanity check to ensure none of our own variables will leak into the output
        for k, v in local_vars.items():
            assert k.startswith("_")
        self.current_row = i
        for key, values in self.input_values.items():
            yield key, _ValueWrapper.wrap_recursive(key, values[i])

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
            if isinstance(value, _ValueWrapper):
                # Top-level object. It is either untouched or the result of an assignment like `x = y`.
                # We need to store it in either case to handle code like
                # `if x % 2 == 0: x = x/2` that only modifies part of
                # a column.
                self.output_values[key].append(value._wrapped_obj)
                if key != value._fieldname:
                    self.changed.add(key)
            elif isinstance(value, Box):
                for flat_key, value, was_touched in _flatten_box(key, value):
                    self.output_values[flat_key].append(value)
                    if was_touched:
                        self.changed.add(flat_key)                
            else:
                # Top-level object that was overwritten.
                self.output_values[key].append(value)
                self.changed.add(key)
        # Manual scope cleanup, to prevent user-defined variables
        # from accidentally surviving into the next loop iteration.
        for key in new_locals:
            local_vars.pop(key, None)

    def finish(self) -> pa.RecordBatch:
        output_data: Dict[str, Tuple[pa.Field, pa.Array]] = {}
        # Construct flattened output batch. This implicitly handles deleted
        # fields as well, since those won't have any recorded output values:
        for field, array in _flatten_batch(self.original_batch):
            if field.name in self.output_values:
                output_data[field.name] = (field, array)
        # Overwrite the fields that were changed by the user code.
        for key, values in self.output_values.items():
            if key not in self.changed:
                continue
            example_value = _find_first_nonnull(values)
            if example_value is None:
                # TODO: The user assigned only `None` to some field. It's unclear
                # what to do here: If the field already existed we could get
                # the type from the input batch, if it's a new field we can
                # probably raise an exception?
                del output_data[key]
                continue
            type_ = infer_type(example_value)
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
    codepipe = int(sys.argv[1])
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
