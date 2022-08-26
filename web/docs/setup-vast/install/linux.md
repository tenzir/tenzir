# Linux

Use our [pre-built build packages](/docs/setup-vast/download#packages) or [build
from source](/docs/setup-vast/build) to install VAST on any Linux distribution.

To deploy VAST as system service, you can use our [systemd
configuration](#systemd).

## systemd

VAST has native [systemd
support](https://github.com/tenzir/vast/tree/master/systemd/) for running as
system service. The service is sandboxed and runs with limited privileges.

### Prepare the host system

Please note that all subsequent commands require `root` privileges. The service
requires a user and group called `vast`. You can create them as follows.

```bash
useradd --system --user-group vast
```

Make sure that you don't grant any special rights to this user, e.g., do not
enable `sudo` or other privileged commands. Once the user exists, you should
create the directory for VAST's persistent data and change the permissions such
that it is owned by the new `vast` user:

```bash
mkdir -p /var/lib/vast
chown -R vast:vast /var/lib/vast
```

The systemd unit passes a
[vast.yaml](https://github.com/tenzir/vast/tree/master/systemd/) configuration
file to the VAST process. Make sure that the new user can read the `vast.yaml`.

### Configure the unit

Before you begin, find the line beginning with `ExecStart=` at the very bottom
of the `[Service]` section in the unit file. Depending on your installation path
you might need to change the location of the `vast` binary and configuration
file.

```config
ExecStart=/path/to/vast start
```

In case your VAST deployment needs elevated privileges, e.g., to capture
packets, you can provide them as follows:

```ini
CapabilityBoundingSet=CAP_NET_RAW
AmbientCapabilities=CAP_NET_RAW
```

Then link the unit file to your systemd search path:

```bash
systemctl link vast.service
```

To have the service start up automatically on system boot, `enable` it via
`systemd`. Otherwise, just `start` it to run it immediately.

```bash
systemctl enable vast
systemctl start vast
```

## Distribution Support

### Debian

We provide [pre-built packages](/docs/setup-vast/download#debian) for Debian
and Debian derived distributions. After downloading, you can install VAST with

```bash
dpkg -i vast-${version}_amd64.deb
```

The debian package will automatically create a `vast` system user and install
the systemd server service.

:::tip Community contributions wanted!
We are striving to bring VAST into the package managers of all major Linux
distributions. Unfortunately we can do so only at a best-effort basis, but
we much appreciate community contributions.
:::
