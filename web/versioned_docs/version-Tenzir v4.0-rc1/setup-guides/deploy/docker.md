---
sidebar_position: 0
---

# Docker

Our Docker image contains a dynamic of Tenzir build with plugins as shared
libraries. The system user `tenzir` runs the Tenzir executable with limited
privileges. Database contents go into the volume exposed at `/var/lib/tenzir`.

Make sure you have the appropriate Docker runtime setup, e.g., [Docker
Desktop](https://www.docker.com/products/docker-desktop/) or [Docker
Engine](https://docs.docker.com/engine/).

:::tip CPU & RAM
Make sure Docker has enough multiple CPU cores and several GBs of RAM.
:::
## Pull the image

Retrieving a dockerized version of Tenzir only requires pulling a pre-built
image from our [container registry at DockerHub][dockerhub]:

```bash
docker pull tenzir/tenzir
```

:::tip Slim Images
We also provide slim images that contain only the necessary dependencies to
run tenzir. You can get them by appending the `-slim` suffix to the tag you want
to pull, e.g.:

```bash
docker pull tenzir/tenzir:latest-slim
```
:::

Thereafter, you're ready to start a Tenzir node in a container.

[dockerhub]: https://hub.docker.com/repository/docker/tenzir/tenzir

## Start the container

When running Tenzir in a container, you need to wire two resources for a
practical deployment:

1. **Network**: Tenzir exposes a listening socket to accept client commands.
2. **Disk**: Tenzir stores its database content on mounted volume.

We recommend starting the Tenzir server detached in the background:

```bash
mkdir -p /path/to/db
docker run -dt --name=tenzir --rm -p 5158:5158 -v /path/to/db:/var/lib/tenzir \
  tenzir/tenzir start
```

The `docker` arguments have the following meaning:

- `-d` for detaching, i.e., running in background
- `-t` for terminal output
- `--name` to name the image
- `--rm` to remove the container after exiting
- `-p` to expose the port to the outer world
- `-v from:to` to mount the local path `from` into the container at `to`

Now you are ready to interact with a running Tenzir node.

## Configure a Tenzir container

Configuration in the Docker ecosystem typically entails setting environment
variables. Tenzir supports this paradigm with a one-to-one [mapping from
configuration file entries to environment
variables](../../command-line.md#environment-variables).

When starting the container, `docker run` accepts variables either one by one
via `-e KEY=VALUE` or via `--env-file FILE`. You can also pass down an existing
environment variable by specifying just `-e KEY` without an assignment. Here is
an example:

```bash
docker run -e TENZIR_ENDPOINT -e TENZIR_IMPORT__BATCH_SIZE=42 --env-file .env \
  tenzir/tenzir start
```

## Build your own Tenzir image

You can always build your own Docker image in case our prebuilt images don't fit
your use case.

Our official [Dockerfile](https://github.com/tenzir/tenzir/blob/main/Dockerfile)
offers two starting points: a *development* and *production* layer.

Before building the image, make sure to fetch all submodules:

```bash
git clone --recursive https://github.com/tenzir/tenzir
cd tenzir
```

### Build the production image

The production image is optimized for size and security. This is the official
`tenzir/tenzir` image. From the repository root, build it as follows:

```bash
docker build -t tenzir/tenzir .
```

### Build the development image

The development image `tenzir/tenzir-dev` contains all build-time dependencies
of Tenzir. It runs with a `root` user to allow for building custom images that
build additional Tenzir plugins. By default, Tenzir loads all installed plugins
in our images.

Build the development image by specifying it as `--target`:

```bash
docker build -t tenzir/tenzir-dev --target development .
```
