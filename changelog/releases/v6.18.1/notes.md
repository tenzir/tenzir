This release fixes a memory leak in to_udp, a crash when TCP clients disconnect during accept, and spurious error logs when stopping pipelines that use the python operator.

## 🐞 Bug fixes

### Memory leak in `to_udp`

The `to_udp` operator no longer leaks memory for every batch of events it sends. The leak affected builds compiled with GCC 15 and grew steadily in long-running pipelines.

*By @tobim.*

### No crash when a TCP client disconnects during accept

The node no longer crashes when a TCP client connects to a listening operator and resets the connection before the node has finished accepting it. Previously, this terminated the entire node with the message `setFromSocket() failed: Socket not connected`. Port scanners, load balancer health checks, and clients with short connect timeouts could trigger this. This affects all operators that accept incoming TCP connections, namely `accept_tcp`, `accept_relp`, and `serve_tcp`.

*By @tobim.*

### Spurious import errors when stopping pipelines that use `python`

Stopping a pipeline while its `python` operator is still starting no longer logs misleading Python tracebacks such as `ModuleNotFoundError: No module named 'tenzir_operator'`. The operator now shuts down its Python process before it removes the process's virtual environment.

*By @tobim.*
