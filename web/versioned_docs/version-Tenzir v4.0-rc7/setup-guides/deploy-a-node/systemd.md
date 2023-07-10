# systemd

The Tenzir package bundles a systemd service unit under
`<extraction_path>/lib/systemd/system/tenzir.service`. The service is sandboxed
and runs with limited privileges.

## Prepare the host system

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

## Configure the unit

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
systemctl link tenzir.service
```

To have the service start up automatically on system boot, `enable` it via
`systemd`. Otherwise, just `start` it to run it immediately.

```bash
systemctl enable tenzir
systemctl start tenzir
```
