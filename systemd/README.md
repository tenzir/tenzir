VAST Systemd Unit
=================

The `vast.service` provides a `systemd` service unit for running VAST as system
service. The service is sandboxed and runs with limited privileges.

## Prepare the Host System

Please note that all subsequent commands require `root` privileges. The service
requires a user and group called `vast`. You can create them as follows.

```bash
useradd --system --user-group vast
```

Make sure that you don't grant any special rights to this user, i.e., do not
enable `sudo` or other privileged commands for this user.

Once the user exists, you should create the directory for VAST's persistent data
storage and change the permissions such that it is owned by the new `vast` user.

```bash
mkdir -p {/var/db/vast,/var/log/vast}
chown -R vast:vast {/var/db/vast,/var/log/vast}
```

As described above, the systemd unit is configured to allow certain write paths
for logging and file storage. This also has to be configured in VAST. Use the
provided
[vast.yaml](https://github.com/tenzir/vast/tree/master/systemd/vast.yaml) file.

The default configuration directory for VAST is `/etc/vast`. Place the
configuration file in there. Make sure that the new `vast` user can read the
contents of `/etc/vast/vast.yaml`.

## Usage

Before you begin, find the line beginning with `ExecStart=` at the very bottom
of the `[Service]` section in the unit file. Depending on your installation path
you might need to change the location of the `vast` binary and configuration
file.

```bash
ExecStart=/path/to/vast --config=/path/to/vast.yaml start
```

Then copy (or symlink) the unit file to `/etc/systemd/system`.

```bash
ln -s $(echo $PWD)/vast.service /etc/systemd/system
```

To have the service start up automatically with system boot, you can `enable` it
via `systemd`. Otherwise, just `start` it to run it immediately.

```bash
systemctl enable vast
systemctl start vast
```
