---
sidebar_position: 2
---

# Spawn a node

A *node* is a managed service for pipelines and storage.

## Start a node

Simply run the `tenzir-node` executable to start a node:

```bash
tenzir-node
```

This will spawn a blocking process that listens by default on the TCP endpoint
127.0.0.1:5158. Select a different endpoint via `--endpoint`, e.g., bind to an
IPv6 address:

```bash
tenzir-node --endpoint=[::1]:42000
```

Check out the various [deployment options](../setup-guides/deploy/README.md) for
other methods of spinning up a node, e.g., via systemd, Ansible, or Docker.

## Stop a node

There exist two ways stop a server:

1. Hit CTRL+C in the same TTY where you started Tenzir.
2. Send the process a SIGINT or SIGTERM signal, e.g., via `pkill -2 tenzir`.

Sending the process a SIGTERM is the same as hitting CTRL+C.

:::tip Easy-Button Nodes
Want it managed? Head to [tenzir.com](https://tenzir.com) and sign up for the
free Community Edition to experience easy-button node management in the
browser.
:::
