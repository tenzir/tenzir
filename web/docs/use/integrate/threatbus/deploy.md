---
sidebar_position: 2
---

# Deploy

This section explains how to run Threat Bus.

## systemd

We provide `systemd` service units to run
[Threat Bus](https://pypi.org/project/threatbus/) and
[VAST Threat Bus](https://pypi.org/project/vast-threatbus/) as
system services. The services are sandboxed and run with limited privileges.

The systemd units declare a private user. Hence, all logs go to
`/var/log/private` by default. The following section explains how to configure
file-logging for Threat Bus and VAST Threat Bus. Skip the following
instructions if you configure your applications to use console-logging.

Find the `logging` config section at the top of your Threat Bus or VAST Threat
Bus configuration file and change it to use the private log directory:

- `/var/log/private/threatbus/threatbus.log` (Threat Bus)
- `/var/log/private/vast-threatbus/vast-threatbus.log` (VAST Threat Bus)

See the following YAML snippet for a configuration example.

```yaml
logging:
  console: false
  console_verbosity: INFO
  file: true
  file_verbosity: DEBUG
  filename: /var/log/private/threatbus/threatbus.log
```

Before you begin, find the line beginning with `ExecStart=` at the very bottom
of the `[Service]` section in the unit file. Depending on your installation path
you might need to change the location of the `threatbus` and `vast-threatbus`
executable packages and configuration files. Similarly, you need to change the
environmentvariables `THREATBUSDIR` and `VAST_THREATBUSDIR` according to your
installation paths.

- Threat Bus
  ```bash
  Environment="THREATBUSDIR=/installation/path"
  ExecStart=/installation/path/threatbus --config=/installation/path/threatbus/config.yaml
  ```

- VAST Threat Bus
  ```bash
  Environment="VAST_THREATBUSDIR=/installation/path"
  ExecStart=/installation/path/vast-threatbus --config=/installation/path/vast-threatbus/config.yaml
  ```

Then copy (or symlink) the unit file to `/etc/systemd/system`.

```bash
systemctl link "$PWD/threatbus.service"
systemctl link "$PWD/vast-threatbus.service"
```

To have the services start up automatically with system boot, you can `enable`
them via `systemd`. Otherwise, just `start` it to run it immediately.

```bash
systemctl enable threatbus
systemctl start threatbus
systemctl enable vast-threatbus
systemctl start vast-threatbus
```

## Docker

Threat Bus ships as pre-built [Docker
image](https://hub.docker.com/r/tenzir/threatbus). It can be used without any
modifications to the host system. The Threat Bus executable is used as the
entry-point of the container. You can transparently pass all command line
options of Threat Bus to the container.

```bash
docker pull tenzir/threatbus:latest
docker run tenzir/threatbus:latest --help
```

The pre-built image comes with all required dependencies and all existing
plugins pre-installed. Threat Bus requires a config file to operate. That file
has to be made available inside the container, for example via mounting it.

The working directory inside the container is `/opt/tenzir/threatbus`. To mount
a local file named `my-custom-config.yaml` from the current directory into the
container, use the `--volume` (`-v`) flag.

```bash
docker run -v $PWD/my-custom-config.yaml:/opt/tenzir/threatbus/my-custom-config.yaml tenzir/threatbus:latest -c my-custom-config.yaml
```

See the [configuration section](configure) to get started with a custom config
file or refer to the detailed [plugin
documentation](understand/plugins) for fine tuning.

Depending on the installed plugins, Threat Bus binds ports to the host system.
The used ports are defined in your configuration file. When running Threat
Bus inside a container, the container needs to bind those ports to the host
system. Use the `--port` (`-p`) flag repeatedly for all ports you need to bind.

```bash
docker run -p 47661:47661 -p 12345:12345 -v $PWD/config.yaml:/opt/tenzir/threatbus/config.yaml tenzir/threatbus:latest -c config.yaml
```
