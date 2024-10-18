# files

Shows file information for a given directory.

```tql
files [dir:str, recurse=bool, follow_symlinks=bool, skip_permission_denied=bool]
```

## Description

The `files` operator shows file information for all files in the given
directory.

### `dir: str (optional)`

The directory to list files in.

Defaults to the current working directory.

### `recurse = bool (optional)`

Recursively list files in subdirectories.

### `follow_symlinks = bool (optional)`

Follow directory symlinks.

### `skip_permission_denied = bool (optional)`

Skip directories that would otherwise result in permission denied errors.

## Schemas

Tenzir emits file information with the following schema.

### `tenzir.file`

Contains detailed information about the file.

| Field             | Type     | Description                              |
| :---------------- | :------- | :--------------------------------------- |
| `path`            | `string` | The file path.                           |
| `type`            | `string` | The type of the file (see below).        |
| `permissions`     | `record` | The permissions of the file (see below). |
| `owner`           | `string` | The file's owner.                        |
| `group`           | `string` | The file's group.                        |
| `file_size`       | `uint64` | The file size in bytes.                  |
| `hard_link_count` | `uint64` | The number of hard links to the file.    |
| `last_write_time` | `time`   | The time of the last write to the file.  |

The `type` field can have one of the following values:

| Value       | Description                     |
| :---------- | :------------------------------ |
| `regular`   | The file is a regular file.     |
| `directory` | The file is a directory.        |
| `symlink`   | The file is a symbolic link.    |
| `block`     | The file is a block device.     |
| `character` | The file is a character device. |
| `fifo`      | The file is a named IPC pipe.   |
| `socket`    | The file is a named IPC socket. |
| `not_found` | The file does not exist.        |
| `unknown`   | The file has an unknown type.   |

The `permissions` record contains the following fields:

| Field    | Type     | Description                         |
| :------- | :------- | :---------------------------------- |
| `owner`  | `record` | The file permissions for the owner. |
| `group`  | `record` | The file permissions for the group. |
| `others` | `record` | The file permissions for others.    |

The `owner`, `group`, and `others` records contain the following fields:

| Field     | Type   | Description                     |
| :-------- | :----- | :------------------------------ |
| `read`    | `bool` | Whether the file is readable.   |
| `write`   | `bool` | Whether the file is writeable.  |
| `execute` | `bool` | Whether the file is executable. |

## Examples

Compute the total file size of the current directory:

```tql
files recurse=true
summarize total_size=sum(file_size)
```

Find all named pipes in `/tmp`:

```tql
files "/tmp", recurse=true, skip_permission_denied=true
where type == "fifo"
```
