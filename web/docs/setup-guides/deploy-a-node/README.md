# Deploy a node

A *node* is a managed service for pipelines and storage.

## Install a node

Choose between binary package and a Docker-based setup to install a node.

### Binary (Automatic)

Use our installer to install a binary package on any Linux distribution:

```bash
curl https://get.tenzir.app | sh
```

The shell script asks for confirmation before performing the installation.

### Binary (Manual)

If you prefer a manual installation you can also perform the installer steps
yourself.

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs>
<TabItem value="debian" label="Debian">

Download the latest [Debian package][tenzir-debian-package] and install it via
`dpkg`:

```bash
dpkg -i tenzir-static-amd64-linux.deb
```

[tenzir-debian-package]: https://github.com/tenzir/tenzir/releases/latest/download/tenzir-static-amd64-linux.deb

</TabItem>
<TabItem value="nix" label="Nix">

Use our `flake.nix`:

```bash
nix run github:tenzir/tenzir/stable
```

Install Tenzir by adding `github:tenzir/tenzir/stable` to your flake inputs, or
use your preferred method to include third-party modules on classic NixOS.

</TabItem>
<TabItem value="linux" label="Linux">

Download a tarball with our [static binary][tenzir-tarball] for all Linux
distributions and unpack it into `/opt/tenzir`:

```bash
tar xzf tenzir-static-x86_64-linux.tar.gz -C /
```

We also offer prebuilt statically linked binaries for every Git commit to the
`main` branch.

```bash
version="$(git describe --abbrev=10 --long --dirty --match='v[0-9]*')"
curl -fsSL "https://storage.googleapis.com/tenzir-dist-public/packages/main/tenzir-${version}-linux-static.tar.gz"
```

[tenzir-tarball]: https://github.com/tenzir/tenzir/releases/latest/download/tenzir-static-x86_64-linux.tar.gz

</TabItem>
<TabItem value="macos" label="macOS">

Please use Docker [with
Rosetta](https://levelup.gitconnected.com/docker-on-apple-silicon-mac-how-to-run-x86-containers-with-rosetta-2-4a679913a0d5)
until we offer a native package.

</TabItem>
</Tabs>

### Docker

To run a node in a container environment, we offer a Docker image and Docker
Compose config file. Make sure your container has access to network (to expose a
listening socket) and disk (to read and write data).

<Tabs>
<TabItem value="docker" label="Docker">

Pull the image:

```bash
docker pull tenzir/tenzir
```

Alternatively, you can use our size-optimized "slim" with minimal
dependencies:

```bash
docker pull tenzir/tenzir:latest-slim
```

Now start the node:

```bash
mkdir -p /path/to/db
docker run -dt --name=tenzir --rm -p 5158:5158 -v /path/to/db:/var/lib/tenzir \
  tenzir/tenzir start
```

The `docker` arguments have the following meaning:

- `-d` for detaching, i.e., running in background
- `-t` for terminal output
- `--name` to name the image
- `--rm` to remove the container after exiting
- `-p` to expose the port to the outer world
- `-v from:to` to mount the local path `from` into the container at `to`

</TabItem>
<TabItem value="docker-compose" label="Docker Compose">

Visit the [configurator](https://app.tenzir.com/configurator) to download a
`docker-compose.yaml` configuration file. In the same directory of the
downloaded file, run:

```bash
docker compose up
```

</TabItem>
</Tabs>

We map command line option to [environment
variables](../../command-line.md#environment-variables) for more idiomatic use
in containers.

### Automation

Read our guides on automating the deployment of a node:

- [Systemd](systemd.md)
- [Ansible](ansible.md)

## Start a node

After completing the installation, run the `tenzir-node` executable to start a
node.

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
