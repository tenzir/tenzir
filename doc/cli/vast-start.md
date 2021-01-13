The `start` command spins up a VAST node. Starting a node is the first step when
deploying VAST as a continuously running server. The process runs in the
foreground and uses standard error for logging. Standard output remains unused,
unless the `--print-endpoint` option is enabled.

By default, the `start` command creates a `vast.db` directory in the current
working directory. It is recommended to set the options for the node in the
`vast.yaml` file, such that they are picked up by all client commands as well.

In the most basic form, VAST spawns one server process that contains all core
actors that manage the persistent state, i.e., archive and index. This process
spawns only one "container" actor that we call a `node`.

The `node` is the core piece of VAST that is continuously running in the
background, and can be interacted with using the `import` and `export` commands
(among others). To gracefully stop the node, the `stop` command can be used.

To use VAST without running a central node, pass the `--node` flag to commands
interacting with the node. This is useful mostly for quick experiments, and
spawns an ad-hoc node instead of connecting to one.

Only one node can run at the same time for a given database. This is ensured
using a lock file named `pid.lock` that lives inside the `vast.db` directory.

Further information on getting started with using VAST is available on
[docs.tenzir.com](https://docs.tenzir.com/vast/quick-start/introduction).
