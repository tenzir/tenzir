# Docker

This directory contains Docker and Docker Compose files for VAST and
integrations with related services.

> **Note**
> All examples shown use the [Docker Compose V2 CLI][docker-compose-v2-cli]. If
> using `docker compose` (with a space) does not work for you, try using
> `docker-compose` instead—although we don't test it explicitly, most commands
> should work that way as well.

[docker-compose-v2-cli]: https://docs.docker.com/compose/#compose-v2-and-the-new-docker-compose-command

## Quick Start with Docker Compose

To get up and running with VAST in Docker Compose, simply run `docker compose
up` from the `docker/vast` directory, which fetches the latest version of VAST
from Docker Hub.

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
|docker/vast/docker-compose.yaml|The `vast` service that starts up a VAST server.|
|docker/vast/docker-compose.volume.yaml|Add persistent storage to VAST.|
|docker/vast/docker-compose.web.yaml|Expose the VAST web server.|
|docker/vast/docker-compose.build.yaml|Force VAST to be built from source.|
