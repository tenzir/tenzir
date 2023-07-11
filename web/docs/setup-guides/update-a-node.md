---
sidebar_position: 2
---

# Update a node

To update a [deployed node](./deploy-a-node/README.md), proceed with the
platform-specific instructiosn below.

## Docker

Run the following two commands to update a Docker deployment:

```bash
docker compose down
docker compose up --pull
```

## Linux

Run the installer again via:

```bash
curl https://get.tenzir.app | sh
```

## macOS

Please use Docker [with
Rosetta](https://levelup.gitconnected.com/docker-on-apple-silicon-mac-how-to-run-x86-containers-with-rosetta-2-4a679913a0d5)
until we offer a native package.
