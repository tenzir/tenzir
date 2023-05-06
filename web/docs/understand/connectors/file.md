# file

Loads bytes from a file. Saves bytes to a file.

## Synopsis

Loader:

```
file [-f|--follow] [-m|--mmap] [-t|--timeout=<duration>] <path>
```

Saver:

```
file [-a|--append] [-r|--real-time] [--uds] <path>
```

## Description

The `file` loader acquires raw bytes from a file. The `file` saver writes bytes
to a file or a Unix domain socket.

The default parser for the `file` loader is [`json`](../formats/json.md).

The default printer for the `file` saver is [`json`](../formats/json.md).

### `<path>` (Loader, Saver)

The path to the file to load/save. If intermediate directories do not exist, the
saver will create them.

The path `-` is a reserved value and means stdin for the loader and stdout for
the saver.

### `-f|--follow` (Loader)

Do not stop when the end of file is reached, but rather to wait for additional
data to be appended to the input.

This flag has the semantics of the "tail -f" idiom in Unix.

### `-m|--mmap` (Loader)

Use the `mmap(2)` system call to map the file and produce only one single chunk
of bytes, instead of producing data piecemeal via `read(2)`.

### `-t|--timeout=<duration>` (Loader)

Wait at most for the provided duration when performing a blocking call to the
system call `read(2)`.

This flags comes in handy in combination with `--follow` to produce a steady
pulse of input in the pipeline execution, as input (even if empty) drives the
processing forward.

### `-a|--append` (Saver)

Append to the file instead of overwriting it with a new file.

### `-r|--real-time` (Saver)

Immediately synchronize the file with every chunk of bytes instead of buffering
bytes to batch filesystem write operations.

### `--uds` (Saver)

Interpret `path` as a Unix domain socket and connect to it.

## Examples

Read JSON from stdin via [`from`](../operators/sources/from.md) and convert
it to CSV:

```
from - read json | write csv to stdout
```

Read 1 MiB from a file `/tmp/data` and write the bytes another file `/tmp/1mb`,
blocking if `/tmp/data` is less than 1 MiB until the file reaches this size:

```
load file -f /tmp/data | head 1 Mi | save file /tmp/1mb
```
