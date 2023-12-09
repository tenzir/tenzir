# sockets

Shows a snapshot of open sockets.

## Synopsis

```
sockets
```

## Description

The `sockets` operator shows a snapshot of all currently open sockets.

## Examples

Show process ID, local, and remote IP address of all sockets:

```
sockets
| select pid, local_addr, remote_addr 
```
