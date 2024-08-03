"""User code wrapper of the Tenzir python operator."""

import fcntl
import linecache
import os
import sys
import traceback
from collections import defaultdict
from contextlib import suppress
from types import MethodType, ModuleType, CodeType
from typing import (
    Any,
    Dict,
    Generator,
    Iterable,
    Optional,
    SupportsIndex,
    Tuple,
    TypeVar,
    Union,
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


def log(*args):
    prefix = "debug: "
    z = " ".join(map(str, args))
    y = prefix + " " + z.replace("\n", "\n" + prefix + " ")
    print(y, file=sys.stderr)


def add_note(e: Optional[BaseException], msg: str) -> None:
    if e is None:
        return
    if sys.version_info >= (3, 11):
        e.add_note(msg)
        return
    args = e.args
    arg0 = f"{args[0]}\n{msg}" if args else msg
    e.args = (arg0,) + args[1:]


T = TypeVar("T")


class ListWrapper(BoxList):
    def __init__(self: "ListWrapper", *args) -> None:
        self._modified = False
        super().__init__(*args, box_intact_types=(ListWrapper, DictWrapper))

    def __setattr__(self: "ListWrapper", key: str, value) -> None:
        if key == "_modified":
            self.__dict__["_modified"] = value
        else:
            super().__setattr__(key, value)

    def append(self: "ListWrapper", value) -> None:
        self._modified = True
        super().append(value)

    def __setitem__(self, key, value) -> None:
        self._modified = True
        super().__setitem__(key, value)

    def __delitem__(self: "ListWrapper", key: Union[SupportsIndex, slice]) -> None:
        self._modified = True
        super().__delitem__(key)


class DictWrapper(Box):
    def __init__(self: "DictWrapper", *args, **kwargs) -> None:
        self._modified: set[str] = set()
        kwargs["box_intact_types"] = tuple(
            {ListWrapper, DictWrapper, *kwargs.get("box_intact_types", ())},
        )
        super().__init__(*args, **kwargs)

    def __setattr__(self: "DictWrapper", key: str, value: Any) -> None:
        if key == "_modified":
            self.__dict__["_modified"] = value
        else:
            super().__setattr__(key, value)

    def __setitem__(self: "DictWrapper", key: str, value) -> None:
        self._modified.add(key)
        super().__setitem__(key, value)

    def __delitem__(self: "DictWrapper", key: str) -> None:
        self._modified.add(key)
        return super().__delitem__(key)


def is_modified(obj: object) -> bool:
    if isinstance(obj, DictWrapper):
        return len(obj._modified) > 0 or any(
            is_modified(child) for child in obj.values()
        )
    if isinstance(obj, ListWrapper):
        return obj._modified or any(is_modified(child) for child in obj)
    return False


def reset_modified(obj: object) -> None:
    if isinstance(obj, DictWrapper):
        obj._modified = set()
        for value in obj.values():
            reset_modified(value)
    elif isinstance(obj, ListWrapper):
        obj._modified = False
        for x in obj:
            reset_modified(x)
    else:
        return


def wrap_recursive(fieldname: str, obj: object, fieldtype: pa.DataType):
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
                wrap_recursive(fieldname + "." + key, value, fieldtype.field(key).type),
            )
            for key, value in obj.items()
        )
        return DictWrapper(items)
    if isinstance(obj, list):
        assert isinstance(fieldtype, pa.ListType)
        items = (
            wrap_recursive(f"{fieldname}[{i}]", value, fieldtype.value_type)
            for i, value in enumerate(obj)
        )
        return ListWrapper(items)
    return obj


def flatten_struct_array(field: pa.Field, array: pa.Array) -> Generator:
    if isinstance(array, pa.StructArray):
        for x, y in zip(field.flatten(), array.flatten()):
            yield from flatten_struct_array(x, y)
    else:
        yield field, array


def flatten_batch(batch: pa.RecordBatch) -> Generator:
    for i, field in enumerate(batch.schema):
        array = batch.column(i)
        yield from flatten_struct_array(field, array)


def flatten_object(
    parent_key: str,
    o: object,
    *,
    parent_changed: bool,
) -> Generator[tuple[str, object, bool], None, None]:
    if isinstance(o, DictWrapper):
        for key, value in o.items():
            changed = key in o._modified
            yield from flatten_object(
                f"{parent_key}.{key}",
                value,
                parent_changed=changed,
            )
    elif isinstance(o, ListWrapper):
        # We stop flattening at lists, analogous to `RecordBatch.flatten()`.
        # We still need to continue the recursion to check if anything was
        # modified.
        yield parent_key, o, is_modified(o)
    else:
        yield parent_key, o, parent_changed


def unflatten_nested(box: Box) -> pa.RecordBatch:
    arrays = []
    fields = []
    for name, value in box.items():
        if isinstance(value, Box):
            inner_batch = unflatten_nested(value)
            arrays.append(inner_batch.to_struct_array())
            fields.append(pa.field(name, pa.struct(inner_batch.schema)))
        else:
            field, array = value
            fields.append(field.with_name(name))
            arrays.append(array)
    return pa.RecordBatch.from_arrays(arrays=arrays, schema=pa.schema(fields))


def unflatten_batch(
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
            inner_batch = unflatten_nested(value)
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


def find_first_nonnull(xs: list[Union[T, None]]) -> Union[T, None]:
    return next((x for x in xs if x is not None), None)


class ResultsBuffer:
    """Stores the values produced by the user code.

    Builds a new record batch from the all the scalars with the `finish`
    function.
    """

    def __init__(self: "ResultsBuffer", original_batch: pa.RecordBatch) -> None:
        self.original_batch = original_batch
        self.input_values = original_batch.to_pydict()
        # self.input_values = filter_sporadic_nulls(self.original_batch)
        self.output_values: Dict[str, list] = defaultdict(list)
        self.current_row: int = -1
        self.changed: set[str] = set()

    def start_row(
        self: "ResultsBuffer",
        i: int,
    ) -> DictWrapper:
        self.current_row = i
        # conversion_box: Automatically mangle field names to allow safe access
        #                 (ie. "3rd field" -> out.x3rd_field = 0)
        # default_box: Allow recursive definitions (ie. out.foo.bar = 3)
        out = DictWrapper(
            conversion_box=True, default_box=True, default_box_attr=DictWrapper
        )
        for key, values in self.input_values.items():
            field = self.original_batch.schema.field(key)
            wrapped_value = wrap_recursive(key, values[i], field.type)
            out[key] = wrapped_value
        reset_modified(out)
        return out

    def finish_row(self: "ResultsBuffer", local_vars: DictWrapper) -> None:
        i = self.current_row
        if i < 0:
            msg = f"row index should be >=0, got {i}"
            raise ValueError(msg)
        for key, value in local_vars.items():
            # Ignore `import` statements
            if isinstance(value, ModuleType):
                continue
            # Ignore all variables starting with an underscore.
            changed = key in local_vars._modified
            for flat_key, flat_value, was_touched in flatten_object(
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

    def finish(self: "ResultsBuffer") -> pa.RecordBatch:
        output_data: Dict[str, Tuple[pa.Field, pa.Array]] = {}
        original_fieldnames = set()
        # Construct flattened output batch. This implicitly handles deleted
        # fields as well, since those won't have any recorded output values:
        input_rows = self.original_batch.num_rows
        for field, array in flatten_batch(self.original_batch):
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
            example_value = find_first_nonnull(values)
            try:
                type_ = infer_type(example_value)
                field = pa.field(key, type_)
                array = extension_array(values, type_)
            except Exception as e:
                msg = f"failed to write modified '{key}'"
                add_note(e, msg)
                raise
            output_data[key] = (field, array)

        # Construct final record batch and revert the flattening.
        fields, arrays = zip(*output_data.values())
        return unflatten_batch(fields, arrays)


class WrappedError(Exception):
    pass


def execute_user_code(batch: pa.RecordBatch, compiled_code: CodeType) -> pa.RecordBatch:
    buffer = ResultsBuffer(batch)
    for i in range(batch.num_rows):
        # Create the magic `self` variable.
        self = buffer.start_row(i)
        env = {"self": self}
        # Run the user-provided code
        try:
            exec(compiled_code, env)
        except Exception as e:
            add_note(e, f"on input event: {self}")
            raise WrappedError from e
        if len(self) == 0:
            msg = "Empty output not allowed"
            raise RuntimeError(msg)
        buffer.finish_row(self)
    return buffer.finish()


def write_limited(file, limit: int):
    def limit_write(self, __s) -> int:
        nonlocal limit
        if limit == 0:
            return 0
        if len(__s) > limit:
            __s = __s[:limit]
            limit = 0
        else:
            limit = limit - len(__s)
        return self.__class__.write(self, __s)

    file.write = MethodType(limit_write, file)
    return file


def main() -> int:
    """Run the user provided code on behalf of the operator.

    Expects two open file descriptor numbers as arguments.
    The first is for receiving the user code.
    The second is to write back a message in case an error occurred.
    """
    # The parent uses `codepipe` to transfer the user code to be executed
    # into this program.
    codepipe = os.fdopen(int(sys.argv[1]), "r")
    code = codepipe.read()
    codepipe.close()
    source_name = "<inline>"
    lines = code.splitlines(keepends=True)
    linecache.cache[source_name] = len(code), None, lines, source_name

    # When the parent detects an error, it attempts to read the contents
    # of `errpipe` and aborts the pipeline with them as the error.
    errfd = int(sys.argv[2])
    errpipe_bufsize = 4096
    if sys.platform == "linux" and sys.version_info >= (3, 10):
        errpipe_bufsize = fcntl.fcntl(errfd, fcntl.F_GETPIPE_SZ)
    if sys.platform == "darwin":
        errpipe_bufsize = 65536
    with write_limited(os.fdopen(errfd, "a"), errpipe_bufsize) as errpipe:

        # The parent uses stdin and stdout to transfer the arrow record batches
        # back and forth.
        istream = pa.input_stream(sys.stdin.buffer)
        ostream = pa.output_stream(sys.stdout.buffer)

        try:
            compiled_code = compile(code, source_name, "exec")
        except (SyntaxError, ValueError):
            traceback.print_exc(limit=0, file=errpipe)
            return 1
        try:
            while True:
                reader = pa.ipc.RecordBatchStreamReader(istream)
                batch_in = reader.read_next_batch()
                # The writer writes an invalid record batch as end-of-stream
                # marker; we have to read it now to remove it from the pipe
                # buffer.
                with suppress(StopIteration):
                    reader.read_next_batch()
                batch_out = execute_user_code(batch_in, compiled_code)
                writer = pa.ipc.RecordBatchStreamWriter(ostream, batch_out.schema)
                writer.write_batch(batch_out)
                writer.close()
                sys.stdout.flush()
        except pa.lib.ArrowInvalid:
            # The reader throws `ArrowInvalid` when the input is closed
            # by the parent process.
            pass
        except WrappedError as e:
            inner = e.__cause__
            t = inner.__class__ if inner else None
            tb = inner.__traceback__ if inner else None
            tb = tb.tb_next if tb else tb
            traceback.print_exception(t, inner, tb, file=errpipe)
            return 1
        except BaseException:
            traceback.print_exc(file=errpipe)
            return 1
    return 0
