---
sidebar_position: 2
---

# Update a node

To update a [deployed node](./deploy-a-node.md), proceed with the
platform-specific instructions below.

## Docker

Run the following commands to update a Docker Compose deployment with a
configuration file `docker-compose.NODE.yaml`:

```bash
docker compose -f docker-compose.NODE.yaml pull
docker compose -f docker-compose.NODE.yaml down
docker compose -f docker-compose.NODE.yaml up --detach
```

Note that we `pull` first so that the subsequent downtime between `down` and
`up` is minimal.

## Linux

Run the installer again via:

```bash
curl https://get.tenzir.app | sh
```

## macOS

Please use Docker [with
Rosetta](https://levelup.gitconnected.com/docker-on-apple-silicon-mac-how-to-run-x86-containers-with-rosetta-2-4a679913a0d5)
until we offer a native package.
