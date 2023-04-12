---
sidebar_position: 3
---

# Use

This section covers a brief walk-through of how to get started with Threat Bus.
First, [install Threat Bus](install) and all plugins you need. Use the [default
configuration
file](https://github.com/tenzir/threatbus/blob/master/config.yaml.example) to
get started or [create a custom one](configure).

## Start Up

Display the help text:

```bash
threatbus --help
```

Start Threat Bus (it automatically looks for `config.yaml` or `config.yml` in
the same directory):

```bash
threatbus
```

Pass a configuration file to Threat Bus via `-c <path/to/file>`:

```bash
threatbus -c path/to/config.yaml
```

## Start Zeek as Threat Bus App

[Apps](understand/plugins/apps/zeek) need to register at the bus. Zeek can be
scripted, and the relevant functionality for Zeek to subscribe to Threat Bus is
implemented in [this Zeek
script](https://github.com/tenzir/threatbus/tree/master/apps/zeek). To connect
Zeek with Threat Bus, download and load the Zeek script as follows.

```bash
curl -L -o threatbus.zeek https://raw.githubusercontent.com/tenzir/threatbus/master/apps/zeek/threatbus.zeek
zeek -i <INTERFACE> -C threatbus.zeek
```

### Request an IoC Snapshot with Zeek

Threat Bus allows apps to [request snapshots](understand/snapshotting) of historic
security content. The Zeek script implements this request functionality for
indicators. Invoke it like this.

```bash
zeek -i <INTERFACE> -C threatbus.zeek -- "Tenzir::snapshot_intel=30 days"
```

## Use the Docker Container

Threat Bus can be used in a containerized setup. The pre-built
[docker image](https://hub.docker.com/r/tenzir/threatbus) comes with all
required dependencies and all existing plugins pre-installed.

```bash
docker run tenzir/threatbus:latest --help
```
