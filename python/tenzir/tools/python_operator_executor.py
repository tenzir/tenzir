"""User code wrapper of the Tenzir python operator."""
import os
import sys
from collections import defaultdict
from types import ModuleType
from typing import (
    Any,
    Dict,
    Generator,
    Iterable,
    Optional,
    SupportsIndex,
    Tuple,
    TypeVar,
)

import pyarrow as pa
from box import Box
from box.box_list import BoxList

from tenzir.utils.arrow import extension_array, infer_type

"""
NOTE: This script is used by and developed alongside the built-in `python`
pipeline operator in libtenzir, and has little use outside of that narrow use-case.

The intended use case for this is to use the tenzir sources directly
while developing. That can be done conveniently with
```bash
export TENZIR_PLUGINS__PYTHON__IMPLICIT_REQUIREMENTS="-e /path/to/tenzir/python[operator]"
```
"""


def _log(*args):
    prefix = "debug: "
    z = " ".join(map(str, args))
    y = prefix + " " + z.replace("\n", "\n" + prefix + " ")
    print(y, file=sys.stderr)


T = TypeVar("T")


class _ListWrapper(BoxList):
    def __init__(self: "_ListWrapper", *args) -> None:
        self._modified = False
        super().__init__(*args, box_intact_types=(_ListWrapper, _DictWrapper))

    def __setattr__(self: "_ListWrapper", key: str, value) -> None:
        if key == "_modified":
            self.__dict__["_modified"] = value
        else:
            super().__setattr__(key, value)

    def append(self: "_ListWrapper", value) -> None:
        self._modified = True
        super().append(value)

    def __setitem__(self, key, value) -> None:
        self._modified = True
        super().__setitem__(key, value)

    def __delitem__(self: "_ListWrapper", key: SupportsIndex | slice) -> None:
        self._modified = True
        super().__delitem__(key)


class _DictWrapper(Box):
    def __init__(self: "_DictWrapper", *args, **kwargs) -> None:
        self._modified: set[str] = set()
        kwargs["box_intact_types"] = tuple(
            {_ListWrapper, _DictWrapper, *kwargs.get("box_intact_types", ())},
        )
        super().__init__(*args, **kwargs)

    def __setattr__(self: "_DictWrapper", key: str, value: Any) -> None:
        if key == "_modified":
            self.__dict__["_modified"] = value
        else:
            super().__setattr__(key, value)

    def __setitem__(self: "_DictWrapper", key: str, value) -> None:
        self._modified.add(key)
        super().__setitem__(key, value)

    def __delitem__(self: "_DictWrapper", key: str) -> None:
        self._modified.add(key)
        return super().__delitem__(key)


def _is_modified(obj: object) -> bool:
    if isinstance(obj, _DictWrapper):
        return len(obj._modified) > 0 or any(
            _is_modified(child) for child in obj.values()
        )
    if isinstance(obj, _ListWrapper):
        return obj._modified or any(_is_modified(child) for child in obj)
    return False


def _reset_modified(obj: object) -> None:
    if isinstance(obj, _DictWrapper):
        obj._modified = set()
        for value in obj.values():
            _reset_modified(value)
    elif isinstance(obj, _ListWrapper):
        obj._modified = False
        for x in obj:
            _reset_modified(x)
    else:
        return


def _wrap_recursive(fieldname: str, obj: object, fieldtype: pa.DataType):
    """Wrap a nested structure of fields.

    e.g. {"foo": [1,2,3]} -> _DictWrapper("foo": _ListWrapper([1,2,3]))
    The wrappers track if their elements were modified.
    """
    # For a struct where all inner fields are none, `RecordBatch.to_pydict()` annoyingly
    # creates just a single `None` value, as opposed to a full struct
    if isinstance(fieldtype, pa.StructType) and obj is None:
        obj = {inner.name: None for inner in fieldtype}

    if isinstance(obj, dict):
        assert isinstance(fieldtype, pa.StructType)
        items = (
            (
                key,
                _wrap_recursive(
                    fieldname + "." + key, value, fieldtype.field(key).type
                ),
            )
            for key, value in obj.items()
        )
        return _DictWrapper(items)
    if isinstance(obj, list):
        assert isinstance(fieldtype, pa.ListType)
        items = (
            _wrap_recursive(f"{fieldname}[{i}]", value, fieldtype.value_type)
            for i, value in enumerate(obj)
        )
        return _ListWrapper(items)
    return obj


def _flatten_struct_array(field: pa.Field, array: pa.Array) -> Generator:
    if isinstance(array, pa.StructArray):
        for x, y in zip(field.flatten(), array.flatten()):
            yield from _flatten_struct_array(x, y)
    else:
        yield field, array


def _flatten_batch(batch: pa.RecordBatch) -> Generator:
    for i, field in enumerate(batch.schema):
        array = batch.column(i)
        yield from _flatten_struct_array(field, array)


def _flatten_object(
    parent_key: str,
    o: object,
    *,
    parent_changed: bool,
) -> Generator[tuple[str, object, bool], None, None]:
    if isinstance(o, _DictWrapper):
        for key, value in o.items():
            changed = key in o._modified
            yield from _flatten_object(
                f"{parent_key}.{key}",
                value,
                parent_changed=changed,
            )
    elif isinstance(o, _ListWrapper):
        # We stop flattening at lists, analogous to `RecordBatch.flatten()`.
        # We still need to continue the recursion to check if anything was
        # modified.
        yield parent_key, o, _is_modified(o)
    else:
        yield parent_key, o, parent_changed


def _unflatten_nested(box: Box) -> pa.RecordBatch:
    arrays = []
    fields = []
    for name, value in box.items():
        if isinstance(value, Box):
            inner_batch = _unflatten_nested(value)
            arrays.append(inner_batch.to_struct_array())
            fields.append(pa.field(name, pa.struct(inner_batch.schema)))
        else:
            field, array = value
            fields.append(field.with_name(name))
            arrays.append(array)
    return pa.RecordBatch.from_arrays(arrays=arrays, schema=pa.schema(fields))


def _unflatten_batch(
    fields: Iterable[pa.Field],
    arrays: Iterable[pa.Array],
) -> pa.RecordBatch:
    """Reverts the unflattening performed by _flatten_batch()`."""
    outbox = Box(default_box=True, box_dots=True)
    for field, array in zip(fields, arrays):
        outbox[field.name] = (field, array)
    unflat_fields = []
    unflat_arrays = []
    for key, value in outbox.items():
        if isinstance(value, Box):
            inner_batch = _unflatten_nested(value)
            field = pa.field(key, pa.struct(inner_batch.schema))
            array = inner_batch.to_struct_array()
        else:
            field, array = value
        unflat_fields.append(field)
        unflat_arrays.append(array)
    return pa.RecordBatch.from_arrays(
        arrays=unflat_arrays,
        schema=pa.schema(unflat_fields),
    )


def _find_first_nonnull(xs: list[T | None]) -> T | None:
    return next((x for x in xs if x is not None), None)


class _ResultsBuffer:
    """Stores the values produced by the user code.

    Builds a new record batch from the all the scalars with the `finish`
    function.
    """

    def __init__(self: "_ResultsBuffer", original_batch: pa.RecordBatch) -> None:
        self.original_batch = original_batch
        self.input_values = original_batch.to_pydict()
        # self.input_values = filter_sporadic_nulls(self.original_batch)
        self.output_values: Dict[str, list] = defaultdict(list)
        self.current_row: int = -1
        self.changed: set[str] = set()

    def start_row(
        self: "_ResultsBuffer",
        i: int,
    ) -> _DictWrapper:
        self.current_row = i
        # conversion_box: Automatically mangle field names to allow safe access
        #                 (ie. "3rd field" -> out.x3rd_field = 0)
        # default_box: Allow recursive definitions (ie. out.foo.bar = 3)
        out = _DictWrapper(
            conversion_box=True, default_box=True, default_box_attr=_DictWrapper
        )
        for key, values in self.input_values.items():
            field = self.original_batch.schema.field(key)
            wrapped_value = _wrap_recursive(key, values[i], field.type)
            out[key] = wrapped_value
        _reset_modified(out)
        return out

    def finish_row(self: "_ResultsBuffer", local_vars: _DictWrapper) -> None:
        i = self.current_row
        if i < 0:
            msg = f"row index should be >=0, got {i}"
            raise ValueError(msg)
        for key, value in local_vars.items():
            # Ignore `import` statements
            if isinstance(value, ModuleType):
                continue
            # Ignore all variables starting with an underscore.
            if key.startswith("_"):
                continue
            changed = key in local_vars._modified
            for flat_key, flat_value, was_touched in _flatten_object(
                key,
                value,
                parent_changed=changed,
            ):
                # The value may or may not have been changed by the user code.
                # We need to store it in either case to handle code like
                # `if x % 2 == 0: x = x/2` that only modifies part of a column.
                values = self.output_values[flat_key]
                for _ in range(0, i - len(values)):
                    values.append(None)
                values.append(flat_value)
                if was_touched:
                    self.changed.add(flat_key)

    def finish(self: "_ResultsBuffer") -> pa.RecordBatch:
        output_data: Dict[str, Tuple[pa.Field, pa.Array]] = {}
        original_fieldnames = set()
        # Construct flattened output batch. This implicitly handles deleted
        # fields as well, since those won't have any recorded output values:
        input_rows = self.original_batch.num_rows
        for field, array in _flatten_batch(self.original_batch):
            original_fieldnames.add(field.name)
            if field.name in self.output_values:
                output_data[field.name] = (field, array)

        # Overwrite the fields that were changed by the user code.
        for key, values in self.output_values.items():
            # Skip fields that were unchanged from the input.
            if key in original_fieldnames and key not in self.changed:
                continue
            # Fix up missing some output at the end.
            for _ in range(0, input_rows - len(values)):
                values.append(None)
            # Build the new output array
            example_value = _find_first_nonnull(values)
            try:
                type_ = infer_type(example_value)
                field = pa.field(key, type_)
                array = extension_array(values, type_)
            except Exception as e:
                raise Exception(f"failed to write modified '{key}': {e}")
            output_data[key] = (field, array)

        # Construct final record batch and revert the flattening.
        fields, arrays = zip(*output_data.values())
        return _unflatten_batch(fields, arrays)


def _execute_user_code(__batch: pa.RecordBatch, __code: str) -> pa.RecordBatch:
    __buffer = _ResultsBuffer(__batch)
    for __i in range(__batch.num_rows):
        # Create the magic `self` variable.
        self = __buffer.start_row(__i)
        # == Run the user-provided code ===========
        exec(__code, globals(), locals())
        # =========================================
        if len(self) == 0:
            raise Exception("Empty output not allowed")
        __buffer.finish_row(self)
        # Manual scope cleanup, to prevent user-defined variables
        # from accidentally surviving into the next loop iteration.
        new_locals = list(locals().keys())
        for key in new_locals:
            locals().pop(key, None)
    return __buffer.finish()


def main() -> int:
    """Run the user provided code on behalf of the operator.

    Expects two open file descriptor numbers as arguments.
    The first is for receiving the user code.
    The second is to write back a message in case an error occurred.
    """
    # The parent uses `codepipe` to transfer the user code to be executed
    # into this program.
    codepipe = int(sys.argv[1])
    code = os.read(codepipe, 128 * 1024)

    # When the parent detects an error, it attempts to read the contents
    # of `errpipe` and aborts the pipeline with them as the error.
    errpipe = int(sys.argv[2])

    # The parent uses stdin and stdout to transfer the arrow record batches
    # back and forth.
    istream = pa.input_stream(sys.stdin.buffer)
    ostream = pa.output_stream(sys.stdout.buffer)

    try:
        while True:
            reader = pa.ipc.RecordBatchStreamReader(istream)
            batch_in = reader.read_next_batch()
            # The writer writes an invalid record batch as end-of-stream
            # marker; we have to read it now to remove it from the pipe
            # buffer.
            try:
                reader.read_next_batch()
            except StopIteration:
                pass
            batch_out = _execute_user_code(batch_in, code.decode())
            writer = pa.ipc.RecordBatchStreamWriter(ostream, batch_out.schema)
            writer.write_batch(batch_out)
            writer.close()
            sys.stdout.flush()
    except pa.lib.ArrowInvalid as ae:
        # The reader throws `ArrowInvalid` when the input is closed
        # by the parent process.
        pass
    except Exception as e:
        message = str(e).encode("utf-8")
        # Ensure we will never block while trying to write the error message
        # by truncating to a size that's smaller than the OS pipe buffer.
        MAX_MSG_SIZE = 4 * 1024
        if len(message) > MAX_MSG_SIZE:
            message = message[:MAX_MSG_SIZE]
        os.write(errpipe, message)
        os.close(errpipe)
        return 1
    return 0
