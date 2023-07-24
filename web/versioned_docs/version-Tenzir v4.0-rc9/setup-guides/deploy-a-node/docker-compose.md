---
sidebar_position: 1
---

# Docker Compose

:::caution Outdated
These instructions are unfortunately outdated. We plan to update them in the
future. For now, please swing by our [Discord](/discord) for questions on the
Docker Compose integrations.
:::

We offer a range of Docker Compose files for quickly getting up and running with
Tenzir. All mentioned files are in the [`docker`][tenzir-repo-docker] directory of
the Tenzir repository, and require having the repository checked out locally.

[tenzir-repo-docker]: https://github.com/tenzir/tenzir/tree/main/docker

## Quick Start with Docker Compose

To get up and running with Tenzir in Docker Compose, simply run from the
`docker/compose` directory:
```bash
export COMPOSE_FILE=tenzir.yaml
docker compose up
```

This fetches the latest version of Tenzir from Docker Hub.

:::info Cached Images and Containers
By default, Docker aggressively caches images and containers. To prevent Docker
from re-using an image, pass `--pull always` (Compose v2.8+) to `docker compose
up`. Similarly, to prevent Docker from re-using an already built container, pass
`--force-recreate`.
:::

The `docker compose run` command makes interacting with Tenzir inside Docker
Compose easy:

```bash
# Run `tenzir status` in the Docker Compose network.
docker compose run tenzir status

# Import a Suricata Eve JSON file in the Docker Compose network.
# NOTE: When piping to stdin, passing --no-TTY is required.
docker compose run --no-TTY tenzir import suricata < path/to/eve.json

# Run a query against Tenzir.
# NOTE: For commands that check whether input exists on stdin, passing
# --interactive=false is required. This is a bug in Docker Compose.
docker compose run --interactive=false tenzir export json '#schema == "suricata.alert"'
```

The Docker Compose network by default exposes Tenzir on port 5158, allowing for
users to connect to it from outside, e.g., with a local Tenzir binary.

## Override Files

Tenzir's integrations with other services are opt-in, i.e., not loaded by
default. To opt into loading another service, specify its override file when
starting
Docker Compose:

```bash
# Load both Tenzir and Zeek, and the import that sits between the two.
# NOTE: The override file for Zeek does not exist yet, but we plan to add it in
# the near future.
docker compose -f docker/compose/tenzir.yaml \
               -f docker/compose/zeek.yaml \
               -f docker/compose/zeek.tenzir-import.yaml \
               up
```

We currently have the following override files:

|File|Description|
|-|-|
|docker/compose/tenzir.yaml|The `tenzir` service that starts up a Tenzir server including REST API.|
|docker/compose/tenzir.volume.yaml|Add persistent storage to Tenzir.|
|docker/compose/tenzir.build.yaml|Force Tenzir to be built from source.|
|docker/compose/quarto.yaml|Build the Quarto image and run Bash inside.|
|docker/compose/quarto.bind.yaml|Bind mound the Tenzir respository.|
|docker/compose/quarto.tenzir.yaml|Apply settings to connect to the Tenzir service.|
|docker/compose/thehive.yaml|Start TheHive/Cortex with a basic initial setup.|
|docker/compose/thehive.tenzir.yaml|Integrate the Analyzer with the Tenzir service.|
|docker/compose/thehive.app.yaml|Start an integration app for Suricata alerts.|
|docker/compose/misp.yaml|Start MISP with a basic initial setup.|
|docker/compose/misp.proxy.yaml|Add a reverse proxy for dynamic hostnames.|
