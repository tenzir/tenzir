# Run

:::caution Renaming in progress!
We are in the middle of renaming VAST to Tenzir in multiple steps. The
documentation here might not reflect the current project state at the moment.
:::

Running Tenzir means spawning a process of the `tenzir-ctl` executable. A Tenzir
process can operate in two modes:

1. **Server**: runs continuously and listens on a network socket accepting
   connections.
2. **Client**: connects to the server to (1) submit a request and receive a
   response, (2) publish data, or (3) subscribe to data.

:::info Tenzir Node
A server contains a special component called the *node* that acts as container
for pluggable components implemented as
[actors](../../develop/architecture/actor-model.md). In the future, Tenzir will
be able to connect multiple nodes together to create a distributed system.
:::

A standard deployment consists of a server close to the data sources and
multiple clients that publish events and submit queries:

![Client & Server](client-server.excalidraw.svg)

## Start a server

The `start` command spins up a Tenzir server that blocks until told to
[stop](#stop-a-server):

```bash
tenzir-ctl start
```

By default, a Tenzir server listens on localhost and TCP port 5158.

Usually you would invoke `tenzir-ctl start` only for testing purposes in a
terminal. In production you would typically use a service manager, e.g.,
[systemd on Linux](../../setup/install/linux.md#systemd).

## Stop a server

There exist two ways stop a server:

1. Hit CTRL+C in the same TTY where you started Tenzir.
2. Send the process a SIGINT or SIGTERM signal, e.g., via `pkill -2 tenzir`.
   Sending Tenzir a SIGTERM is the same as (1).

Option (3) comes in handy when you are working with a remote Tenzir server.

## Spawn a client

Every command except for `start` is a client command that interacts with a
server. Run `tenzir-ctl help` for a list of available commands.

To select a specific Tenzir server to connect to,
[configure](../../setup/configure.md) the endpoint, e.g., by providing
`--endpoint=host:port` on the command line, exporting the environment variable
`TENZIR_ENDPOINT=host:port`, or setting the configuration option
`tenzir.endpoint: host:port` in your `tenzir.yaml`.

## Client connection failure

In the event of a connection failure, the clients will try to reconnect.
This process can be tuned by the two options in the configuration file:

```yaml
tenzir:
  # The timeout for connecting to a Tenzir server. Set to 0 seconds to wait
  # indefinitely.
  connection-timeout: 5m

  # The delay between two connection attempts. Set to 0 to try connecting
  # without retries.
  connection-retry-delay: 3s
```
