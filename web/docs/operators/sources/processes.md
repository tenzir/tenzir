# processes

Shows a snapshot of running processes.

## Synopsis

```
processes
```

## Description

The `processes` operator shows a snapshot of all currently running processes.

## Examples

Show running processes sorted by how long they've been running:

```
processes
| sort startup desc
```

Show the top five running processes by name:

```
processes
| top name
| head 5
```
