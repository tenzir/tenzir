# save_file

Writes a byte stream to a file.

```tql
save_file path:str, [append=bool, real_time=bool, uds=bool]
```

## Description

Writes a byte stream to a file.

### `path: str`

The file path to write bytes to.

### `append = bool (optional)`

If `true`, appends bytes to the file.
If `false`, overwrites existing file data.

Cannot be specified with `uds=true`.

Defaults to `false`.

### `real_time = bool (optional)`

If `true`, immediately writes data to the file.
If `false`, buffers data and flushes when buffer is filled.

Defaults to `false`.

### `uds = bool (optional)`

Whether to create a UNIX Domain Socket instead of a normal file.

Defaults to `false`.

## Examples

```tql
subscribe "ocsf-feed"
// TODO: Filtering 
save_file "oscf.logs", append=true, real_time=true
```
