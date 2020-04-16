The `stop` command gracefully brings down a VAST server, and is the analog of
the `start` command.

While it is technically possible to shut down a VAST server gracefully by
sending `SIGINT(2)` to the `vast start` process, it is recommended to use `vast
stop` to shut down the server process, as it works over the wire as well and
guarantees a proper shutdown. The command blocks execution until the node has
quit, and returns a zero exit code when it succeeded, making it ideal for use in
launch system scripts.
