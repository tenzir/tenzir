# processes

Shows a snapshot of running processes.

```tql
processes
```

## Description

The `processes` operator shows a snapshot of all currently running processes.

## Schemas

Tenzir emits process information with the following schema.

### `tenzir.process`

Contains detailed information about the process.

|Field|Type|Description|
|:-|:-|:-|
|`name`|`string`|The process name.|
|`command_line`|`list<string>`|The command line of the process.|
|`pid`|`uint64`|The process identifier.|
|`ppid`|`uint64`|The parent process identifier.|
|`uid`|`uint64`|The user identifier of the process owner.|
|`gid`|`uint64`|The group identifier of the process owner.|
|`ruid`|`uint64`|The real user identifier of the process owner.|
|`rgid`|`uint64`|The real group identifier of the process owner.|
|`priority`|`string`|The priority level of the process.|
|`startup`|`time`|The time when the process was started.|
|`vsize`|`uint64`|The virtual memory size of the process.|
|`rsize`|`uint64`|The resident set size (physical memory used) of the process.|
|`swap`|`uint64`|The amount of swap memory used by the process.|
|`peak_mem`|`uint64`|Peak memory usage of the process.|
|`open_fds`|`uint64`|The number of open file descriptors by the process.|
|`utime`|`duration`|The user CPU time consumed by the process.|
|`stime`|`duration`|The system CPU time consumed by the process.|

## Examples

### Show running processes by runtime

```tql
processes
sort -startup
```

### Show the top five running processes by name

```tql
processes
top name
head 5
```
