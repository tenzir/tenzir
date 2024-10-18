# load_file

Loads the contents of the file at `path` as a byte stream.

```tql
load_file path:str, [follow=bool, mmap=bool, timeout=duration]
```

## Description

The `file` loader acquires raw bytes from a file.

### `path: str`

The file path to load from. When `~` is the first character, it will be
substituted with the value of the `$HOME` environment variable.

### `follow = bool (optional)`

Do not stop when the end of file is reached, but rather to wait for additional
data to be appended to the input.

### `mmap = bool (optional)`

Use the `mmap(2)` system call to map the file and produce only one single chunk
of bytes, instead of producing data piecemeal via `read(2)`. This option
effectively gives the downstream parser full control over reads.

<!--
TODO: Add this back once they are ported.

For the [`feather`](TODO) and [`parquet`](TODO) parsers, this significantly
reduces memory usage and improves performance.
-->

### `timeout = duration (optional)`

Wait at most for the provided duration when performing a blocking system call.
This flags comes in handy in combination with `follow=true` to produce a steady
pulse of input in the pipeline execution, as input (even if empty) drives the
processing forward.

## Examples

Load the raw contents of `example.txt`:

```tql
load_file "example.txt"
```
