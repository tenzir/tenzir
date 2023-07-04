# Deploy a node

A *node* is a managed service for pipelines and storage.

## Install a node

Use our installer to walk through the installation steps depending on your
platform:

```bash
curl https://get.tenzir.app | sh
```

We also provide more detailed instructions for the following platforms:

1. [Docker](docker.md)
2. [Docker Compose](docker-compose.md)
3. [Systemd](systemd.md)
4. [Ansible](ansible.md)

## Start a node

After completing the installation, run the `tenzir-node` executable to start a
node.

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs>
<TabItem value="binary" label="Binary" default>

```bash
tenzir-node
```

```
[12:43:22.789] node (v3.1.0-377-ga790da3049-dirty) is listening on 127.0.0.1:5158
```

This will spawn a blocking process that listens by default on the TCP endpoint
127.0.0.1:5158. Select a different endpoint via `--endpoint`, e.g., bind to an
IPv6 address:

```bash
tenzir-node --endpoint=[::1]:42000
```

</TabItem>
<TabItem value="docker" label="Docker">

Expose the port of the listening node and provide a directory for storage:

```bash
mkdir storage
docker run -dt -p 5158:5158 -v storage:/var/lib/tenzir tenzir/tenzir --entry-point=tenzir-node
```

</TabItem>
</Tabs>

:::caution Unsafe Pipelines
Some pipeline operators are inherently unsafe due to their side effects, e.g.,
reading from a file. When such operators run inside a node, you may
involuntarily expose the file system to users that have access to the node, or
when you connect the node to the Tenzir platform and manage it via
[app.tenzir.com](https://app.tenzir.com). This may constitute a security risk.

We therefore forbid pipelines with such side effects by default. If you are
aware of the implications, you can remove this restriction by setting
`tenzir.allow-unsafe-pipelines: true` in the `tenzir.yaml` of the respective
node.
:::

## Stop a node

There exist two ways stop a server:

1. Hit CTRL+C in the same TTY where you ran `tenzir-node`.
2. Send the process a SIGINT or SIGTERM signal, e.g., via
   `pkill -2 tenzir-node`.

Sending the process a SIGTERM is the same as hitting CTRL+C.
