# File

Tenzir supports reading from and writing to files, including non-regular files,
such as [Unix domain sockets](https://en.wikipedia.org/wiki/Unix_domain_socket),
standard input, standard output, and standard error.

![File](file.svg)

When `~` is the first character in the file path, the operator substitutes it
with the `$HOME` environment variable.

:::tip URL Support
The URL scheme `file://` dispatches to
[`load_file`](../../tql2/operators/load_file.md) and
[`save_file`](../../tql2/operators/save_file.md) for seamless URL-style use via
[`from`](../../tql2/operators/from.md) and [`to`](../../tql2/operators/to.md).
:::

## Read a file

Read from a file and parse it in the format applied by the file extension:

```tql
from "/tmp/file.json"
```

The [`from`](../../tql2/operators/from.md) operator automatically decompresses the
file, if the suffix list contains a [supported compression
algorithm](../../tql2/operators/from.md#compression):

```tql
from "/tmp/file.json.gz"
```

Some operators perform better when the entire file arrives as a single block of
bytes, such as the [`yara`](../../tql2/operators/yara.md) operator. In this
case, passing `mmap=true` runs more efficiently:

```tql
from "/sandbox/malware.gz", mmap=true
yara "rule.yaml"
```

## Follow a file

A pipeline typically completes once it reads the end of a file. Pass
`follow=true` to disable this behavior and instead wait for new data written to
it. This is similar to running `tail -f` on a file.

```
from "/tmp/never-ending-stream.ndjson", follow=true
```

## Write a file

Write to a file in the format implied by the file extension:

```tql
version
to "/tmp/tenzir-version.json"
```

The [`to`](../../tql2/operators/to.md) operator automatically compresses the
file, if the suffix list contains a [supported compression
algorithm](../../tql2/operators/to.md#compression):

```tql
version
to "/tmp/tenzir-version.json.bz2"
```

## Append to a file

In case the file exists and you do not want to overwrite it, pass `append=true`
as option:

```tql
from {x: 42}
to "/tmp/event.csv", append=true
```

## Read/write a Unix domain socket

Pass `uds=true` to signal that the file is a Unix domain socket:

```tql
to "/tmp/socket", uds=true
```

When reading from a Unix domain socket, Tenzir automatically figures out whether
the file is regular or a socket:

```tql
from "/tmp/socket"
```
