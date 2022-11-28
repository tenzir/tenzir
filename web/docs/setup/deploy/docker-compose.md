---
sidebar_position: 1
---

# Docker Compose

We offer a range of Docker Compose files for quickly getting up and running with
VAST. All mentioned files are in the [`docker`][vast-repo-docker] directory of
the VAST repository, and require having the repository checked out locally.

[vast-repo-docker]: https://github.com/tenzir/vast/tree/master/docker

:::info Docker Compose V2 CLI
All examples shown use the [Docker Compose V2 CLI][docker-compose-v2-cli]. If
using `docker compose` (with a space) does not work for you, try using
`docker-compose` instead—although we don't test it explicitly, most commands
should work that way as well.
:::

[docker-compose-v2-cli]: https://docs.docker.com/compose/#compose-v2-and-the-new-docker-compose-command

## Quick Start with Docker Compose

To get up and running with VAST in Docker Compose, simply run `docker compose
up` from the `docker/vast` directory, which fetches the latest version of VAST
from Docker Hub.

:::info Cached Images and Containers By default, Docker aggressively caches
images and containers. To prevent Docker from re-using an image, pass `--pull
always` (Compose v2.8+) to `docker compose up`. Similarly, to prevent Docker
from re-using an already built container, pass `--force-recreate`. :::

The `docker compose run` command makes interacting with VAST inside Docker
Compose easy:

```bash
# Run `vast status` in the Docker Compose network.
docker compose run vast status

# Import a Suricata Eve JSON file in the Docker Compose network.
# NOTE: When piping to stdin, passing --no-TTY is required.
docker compose run --no-TTY vast import suricata < path/to/eve.json

# Run a query against VAST.
# NOTE: For commands that check whether input exists on stdin, passing
# --interactive=false is required. This is a bug in Docker Compose.
docker compose run --interactive=false vast export json '#type == "suricata.alert"'
```

The Docker Compose network by default exposes VAST on port 42000, allowing for
users to connect to it from outside, e.g., with a local VAST binary.

## Override Files

VAST's integrations with other services are opt-in, i.e., not loaded by default.
To opt into loading another service, specify its override file when starting
Docker Compose:

```bash
# Load both VAST and Zeek, and the import that sits between the two.
# NOTE: The override file for Zeek does not exist yet, but we plan to add it in
# the near future.
docker compose -f docker/vast/docker-compose.yaml \
               -f docker/zeek/docker-compose.yaml \
               -f docker/zeek/docker-compose.vast-import.yaml \
               up
```

We currently have the following override files:

|File|Description|
|-|-|
|docker/vast/docker-compose.yaml|The `vast` service that starts up a VAST server including REST API.|
|docker/vast/docker-compose.volume.yaml|Add persistent storage to VAST.|
|docker/vast/docker-compose.build.yaml|Force VAST to be built from source.|
|docker/quarto/docker-compose.yaml|Build the Quarto image and run Bash inside.|
|docker/quarto/docker-compose.bind.yaml|Bind mound the VAST respository.|
|docker/quarto/docker-compose.vast.yaml|Apply settings to connect to the VAST service.|
|docker/thehive/docker-compose.yaml|Start TheHive/Cortex with a basic initial setup.|
|docker/thehive/docker-compose.vast.yaml|Integrate the Analyzer with the VAST service.|
|docker/thehive/docker-compose.app.yaml|Start an integration app for Suricata alerts.|
