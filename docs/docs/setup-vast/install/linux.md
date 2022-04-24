# Linux

The `vast` executable acts as both client and server. Typically, a VAST server
runs as persistent system service.

## systemd

VAST ships with a [systemd unit
file](https://github.com/tenzir/vast/tree/master/systemd/) for running VAST as
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

```ini
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
## Docker

:::tip Docker Hub
We provide pre-built Docker images at [dockerhub.com/tenzir][dockerhub].
:::

[dockerhub]: https://hub.docker.com/repository/docker/tenzir/vast

Our Docker image contains a dynamic of VAST build with plugins as shared
libraries. The system user `vast` runs the VAST executable with limited
privileges. Database contents go into the volume exposed at `/var/lib/vast`.

### Start the container

Start VAST in a container and detach it to the background.

```bash
mkdir -p /var/lib/vast
docker run -dt --name=vast --rm -p 42000:42000 -v /var/lib/vast:/var/lib/vast tenzir/vast:latest start
```

### Build an image

Build the `tenzir/vast` image as follows:

```bash
docker build -t tenzir/vast:<TAG>
```

In addition to the `tenzir/vast` image, the development image `tenzir/vast-dev`
contains all build-time dependencies of VAST. It runs with a `root` user to
allow for building custom images that build additional VAST plugins. VAST in the
Docker images is configured to load all installed plugins by default.

You can build the development image as follows:

```bash
docker build -t tenzir/vast-dev:<TAG> --target development
```
