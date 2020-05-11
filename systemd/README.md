VAST Systemd Unit
=================

The `vast.service` provides a `systemd` service unit for running VAST as system
service. The service is sandboxed and run with limited privileges.

## Prepare the Host System

A user and group called `vast` are required for running the service. On Linux
machines you can create one as follows.

```sh
sudo useradd -u 1337 vast   # create a new user called 'vast' with user ID 1337
sudo groupadd vast          # create a new group called 'vast'
sudo usermod -aG vast vast  # add the 'vast' user to the 'vast' group
```

Make sure that you don't grant any special rights to this user, i.e., do not
enable `sudo` or other privileged commands for this user.

Once the user exists, you should create the directory for vast's persistent data
storage and change the permissions such that it is owned by the new `vast` user.

```sh
sudo mkdir -p /var/db/vast
sudo chown -R vast. /var/db/vast
```

Also make sure that the new user can read the `vast.conf` and change permissions
if required.

## Usage

Before you begin, find the line beginning with `ExecStart=` at the very bottom
of the `[Service]` section in the unit file. Depending on your installation path
you might need to change the location of the `vast` binary and configuration
file.

```sh
ExecStart=/path/to/vast --config=/path/to/vast.conf start
```

Then copy (or symlink) the unit file to `etc/systemd/system`.

```sh
sudo ln -s $(echo $PWD)/vast.service /etc/systemd/system
```

To have the service start up automatically with system boot, you can `enable` it
via `systemd`. Otherwise, just `start` it to run it immediately.

```sh
sudo systemctl enable vast
sudo systemctl start vast
```
