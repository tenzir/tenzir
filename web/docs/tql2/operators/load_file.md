# load_file

```
load_file path:str, [follow=bool, mmap=bool, timeout=duration]
```

## Description

Loads the contents of the file at `path` as a byte stream.

### `follow=bool`

Whether to follow symlinks.

### `mmap=bool`

Whether to use `[mmap()](https://en.wikipedia.org/wiki/Mmap)`.

### `timeout=duration`

The `duration` after which if the loading is not finished, the operator errors.

## Examples
