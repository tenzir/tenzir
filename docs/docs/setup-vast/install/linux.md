# Linux

Use our [pre-built packages](#download) below or [build from
source](/docs/setup-vast/build) to install VAST on any Linux distribution. To
deploy VAST as system service, you can use our [systemd
configuration](#systemd).

To package VAST...

## Download

We offer pre-built packages containing a statically linked VAST binary, for the
[latest release](https://github.com/tenzir/vast/releases/latest) and the current
development version:

<div align="center" class="padding-bottom--md">
  <a class="button button--md button--primary margin--sm" href="https://github.com/tenzir/vast/releases/latest/download/vast-linux-static.tar.gz">Release</a>
  <a class="button button--md button--info margin--md" href="https://github.com/tenzir/vast/releases/latest/download/vast-linux-static.tar.gz">Development</a>
</div>

You can also get static builds for a specific git commit. This involves
navigating a bit through the github CI:

1. Browse to the [VAST static workflow][vast-workflow]
2. Click on the latest run, e.g., `Merge pull request...`
3. Scroll to the end of the page
4. Click on the artifact filename, e.g.,
   `vast-v1.0.0-101-g6e7a4ef1a4-linux-static.tar.gz`

[vast-workflow]: https://github.com/tenzir/vast/actions?query=branch%3Amaster+workflow%3A%22VAST%22

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
ExecStart=/path/to/vast --config=/path/to/vast.yaml start
```

In case you plan to run a PCAP source directly inside the VAST server process
via `vast spawn source pcap`, you need to make sure that the VAST process gets
the required privileges to listen on the network interfaces.

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

:::tip Community contributions wanted!
We are striving to bring VAST into the package managers of all major Linux
distributions. Unfortunately we can do so only at a best-effort basis, but
we much appreciate community contributions.
:::

### Debian

You can install [an older version of VAST](https://salsa.debian.org/debian/vast)
via APT:

```bash
apt install vast
```

The installation scripts configure VAST as [systemd
service][debian-vast-systemd-service] and store the database in
`/var/lib/vast/db`. Adapt `/etc/vast/vast.yaml` as you see fit.

[debian-vast-systemd-service]: https://salsa.debian.org/debian/vast/-/blob/master/debian/service
