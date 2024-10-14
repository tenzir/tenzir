# load_file

```tql
load_file path:str, [follow=bool, mmap=bool, timeout=duration]
```

## Description

Loads the contents of the file at `path` as a byte stream.

### `path: str`

The file path to load.

### `follow = bool (optional)`

Whether to follow symlinks.

### `mmap = bool (optional)`

Whether to use [`mmap()`](https://en.wikipedia.org/wiki/Mmap).

### `timeout = duration (optional)`

The `duration` after which if the loading is not finished, the operator errors.

## Examples

```tql
load_file "/var/log/logfile"
read_syslog
```
