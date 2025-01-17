# Linux

We offer a native deployment on various Linux distributions.

## Install a node

Second, choose the Linux tab and proceed as follows:

1. Download the config for your node.
1. Create a directory for the platform configuration.
   ```bash
   mkdir -p /opt/tenzir/etc/tenzir/plugin
   ```
1. Move the downloaded `platform.yaml` configuration file to the directory so
   that the node can find it during startup:
   ```bash
   mv platform.yaml /opt/tenzir/etc/tenzir/plugin
   ```
1. Run the installer and follow the instructions to download and start the node:
   ```bash
   curl https://get.tenzir.app | sh
   ```

The installer script asks for confirmation before performing the installation.
If you prefer a manual installation you can also perform the installer steps
yourself. See the [configuration files
documentation](../../configuration.md#configuration-files) for details on how
the node loads config files at startup.

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs>
<TabItem value="debian" label="Debian">

Download the latest [Debian package][tenzir-debian-package] and install it via
`dpkg`:

```bash
dpkg -i tenzir-static-amd64-linux.deb
```

You can uninstall the Tenzir package via `apt-get remove tenzir`. Use `purge`
instead of `remove` if you also want to delete the state directory and leave no
trace.

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

## Start a node manually

The installer script uses the package manager of your Linux distribution to install the Tenzir package. This typically also creates a systemd unit and starts the node automatically.

For testing, development, our troubleshooting, run `tenzir-node` executable to
start a node manually:

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
