---
sidebar_position: 1
---

# Deploy a node

A *node* is a managed service for pipelines and storage.

## Install a node

Start at [app.tenzir.com](https://app.tenzir.com) and click *Add* in the nodes
pane. Then select your platform.

### Docker

We provide Docker images and a Docker Compose file for a container setup.
Install a node as follows:

1. Enter a name for your node and click the download button to obtain the
   `docker-compose.NODE.yaml` configuration file where `NODE` is the name you
   entered for your node.
2. Run
   ```bash
   docker compose -f docker-compose.NODE.yaml up
   ```

Edit the Docker Compose file and change [environment
variables](../configuration.md#environment-variables) to adjust the
configuration.

### Linux

We offer a native deployment on various Linux distributions.
Install a node as follows:

1. Enter a name for your node and click the download button to obtain a
   `platform.yaml` configuration file.
2. Move the `platform.yaml` to `<sysconfdir>/tenzir/plugin/platform.yaml` so
   that the node can find it during startup where `<sysconfdir>` might be
   `/etc`. See the [configuration files
   documentation](../configuration.md#configuration-files) for more options.
3. Run our installer to install a binary package on any Linux distribution:
   ```bash
   curl https://get.tenzir.app | sh
   ```

The installer script asks for confirmation before performing the installation.
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

You can uninstall the Tenzir package as follows:
```bash
apt-get remove tenzir
```

Use purge instead of remove if you also want to delete the state directory and
leave no trace:
```bash
apt-get purge tenzir
```

[tenzir-debian-package]: https://github.com/tenzir/tenzir/releases/latest/download/tenzir-static-amd64-linux.deb

</TabItem>
<TabItem value="rpm_based" label="RPM-based (RedHat, OpenSUSE, Fedora)">

Download the latest [RPM package][tenzir-rpm-package] and install it via
`rpm`:

```bash
rpm -i tenzir-static-amd64-linux.rpm
```

[tenzir-rpm-package]: https://github.com/tenzir/tenzir/releases/latest/download/tenzir-static-amd64-linux.rpm

</TabItem>
<TabItem value="nix" label="Nix">

Use our `flake.nix`:

```bash
nix run github:tenzir/tenzir/stable
```

Install Tenzir by adding `github:tenzir/tenzir/stable` to your flake inputs, or
use your preferred method to include third-party modules on classic NixOS.

</TabItem>
<TabItem value="any" label="Any">

Download a tarball with our [static binary][tenzir-tarball] for all Linux
distributions and unpack it into `/opt/tenzir`:

```bash
tar xzf tenzir-static-x86_64-linux.tar.gz -C /
```

[tenzir-tarball]: https://github.com/tenzir/tenzir/releases/latest/download/tenzir-static-x86_64-linux.tar.gz

We also offer prebuilt statically linked binaries for every Git commit to the
`main` branch.

```bash
curl -O https://storage.googleapis.com/tenzir-dist-public/packages/main/tarball/tenzir-static-main.gz
```

</TabItem>
</Tabs>

### macOS

Please use Docker [with
Rosetta](https://levelup.gitconnected.com/docker-on-apple-silicon-mac-how-to-run-x86-containers-with-rosetta-2-4a679913a0d5)
until we offer a native package.

## Start a node

:::info Implicit start with Docker
You can skip this step if you use [Docker](#docker) because `docker compose up`
already starts a node for you.
:::

Run the `tenzir-node` executable to start a node:

```bash
tenzir-node
```

```
      _____ _____ _   _ ________ ____
     |_   _| ____| \ | |__  /_ _|  _ \
       | | |  _| |  \| | / / | || |_) |
       | | | |___| |\  |/ /_ | ||  _ <
       |_| |_____|_| \_/____|___|_| \_\

          v4.0.0-rc6-0-gf193b51f1f
Visit https://app.tenzir.com to get started.

[16:50:26.741] node listens for node-to-node connections on tcp://127.0.0.1:5158
[16:50:26.982] node connected to platform via wss://ws.tenzir.app:443/production
```

This will spawn a blocking process that listens by default for node-to-node
connections on the TCP endpoint `127.0.0.1:5158`. Select a different endpoint
via the `tenzir.endpoint` option, e.g., bind to an IPv6 address:

```bash
tenzir-node --endpoint=[::1]:42000
```

Set `tenzir.endpoint` to `false` to disable the endpoint, making the node
exclusively accessible through the Tenzir Platform. This effectively prevents
connections from other `tenzir` or `tenzir-node` processes.

## Stop a node

There exist two ways stop a server:

1. Hit CTRL+C in the same TTY where you ran `tenzir-node`.
2. Send the process a SIGINT or SIGTERM signal, e.g., via
   `pkill -2 tenzir-node`.

Sending the process a SIGTERM is the same as hitting CTRL+C.

## Automate the deployment

Use [systemd](#systemd) or [Ansible](#ansible) to automate the deployment of a
node.

### systemd

The Tenzir package bundles a systemd service unit under
`<extraction_path>/lib/systemd/system/tenzir-node.service`. The service is
sandboxed and runs with limited privileges.

#### Prepare the host system

Please note that all subsequent commands require `root` privileges. The service
requires a user and group called `tenzir`. You can create them as follows.

```bash
useradd --system --user-group tenzir
```

Once the user exists, you should create the directory for Tenzir's persistent
data and change the permissions such that it is owned by the new `tenzir` user:

```bash
mkdir -p /var/lib/tenzir
chown -R tenzir:tenzir /var/lib/tenzir
```

#### Configure the unit

Locate the lines beginning with `ExecStart=` and `ExecStop=` at the bottom
of the `[Service]` section in the unit file. Depending on your installation path
you might need to change the location of the `tenzir` binary.

```
ExecStart=/path/to/tenzir start
```

In case your Tenzir deployment needs elevated privileges, e.g., to capture
packets, you can provide them as follows:

```ini
CapabilityBoundingSet=CAP_NET_RAW
AmbientCapabilities=CAP_NET_RAW
```

Then link the unit file to your systemd search path:

```bash
systemctl link tenzir-node.service
```

To have the service start up automatically on system boot, `enable` it via
`systemd`. Otherwise, just `start` it to run it immediately.

```bash
systemctl enable tenzir-node
systemctl start tenzir-node
```

### Ansible

The Ansible role for Tenzir allows for easy integration of Tenzir into
existing Ansible setups. The role uses either the Tenzir Debian package or
the tarball installation method depending on which is appropriate for the
target environment. The role definition is in the
[`ansible/roles/tenzir`][tenzir-repo-ansible] directory of the Tenzir
repository. You need a local copy of this directory so you can use it in your
playbook.

[tenzir-repo-ansible]: https://github.com/tenzir/tenzir/tree/main/ansible/roles/tenzir

#### Example

This example playbook shows how to run a Tenzir service on the machine
`example_tenzir_server`:

```yaml
- name: Deploy Tenzir
  become: true
  hosts: example_tenzir_server
  remote_user: example_ansible_user
  roles:
    - role: tenzir
      vars:
        tenzir_config_dir: ./tenzir
        tenzir_read_write_paths: [ /tmp ]
        tenzir_archive: ./tenzir.tar.gz
        tenzir_debian_package: ./tenzir.deb
```

#### Variables

##### `tenzir_config_dir` (required)

A path to directory containing a [`tenzir.yaml`](../configuration.md)
relative to the playbook.

##### `tenzir_read_write_paths`

A list of paths that Tenzir shall be granted access to in addition to its own
state and log directories.

##### `tenzir_archive`

A tarball of Tenzir structured like those that can be downloaded from the
[GitHub Releases Page](https://github.com/tenzir/tenzir/releases). This is used
for target distributions that are not based on the `apt` package manager.

##### `tenzir_debian_package`

A Debian package (`.deb`). This package is used for Debian and Debian based
Linux distributions.
