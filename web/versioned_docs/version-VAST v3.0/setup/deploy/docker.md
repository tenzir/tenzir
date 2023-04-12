---
sidebar_position: 0
---

# Docker

Our Docker image contains a dynamic of VAST build with plugins as shared
libraries. The system user `vast` runs the VAST executable with limited
privileges. Database contents go into the volume exposed at `/var/lib/vast`.

Make sure you have the appropriate Docker runtime setup, e.g., [Docker
Desktop](https://www.docker.com/products/docker-desktop/) or [Docker
Engine](https://docs.docker.com/engine/).

:::tip CPU & RAM
Make sure Docker has enough multiple CPU cores and several GBs of RAM.
:::
## Pull the image

Retrieving a dockerized version of VAST only requires pulling a pre-built image
from our [container registry at DockerHub][dockerhub]:

```bash
docker pull tenzir/vast
```

Thereafter, you're ready to start a VAST node in a container.

[dockerhub]: https://hub.docker.com/repository/docker/tenzir/vast

## Start the container

When running VAST in a container, you need to wire two resources for a practical
deployment:

1. **Network**: VAST exposes a listening socket to accept client commands.
2. **Disk**: VAST stores its database content on mounted volume.

We recommend starting the VAST server detached in the background:

```bash
mkdir -p /path/to/db
docker run -dt --name=vast --rm -p 5158:5158 -v /path/to/db:/var/lib/vast \
  tenzir/vast start
```

The `docker` arguments have the following meaning:

- `-d` for detaching, i.e., running in background
- `-t` for terminal output
- `--name` to name the image
- `--rm` to remove the container after exiting
- `-p` to expose the port to the outer world
- `-v from:to` to mount the local path `from` into the container at `to`

Now you are ready to interact with a running VAST node.

## Configure a VAST container

Configuration in the Docker ecosystem typically entails setting environment
variables. VAST supports this paradigm with a one-to-one [mapping from
configuration file entries to environment
variables](../configure.md#environment-variables).

When starting the container, `docker run` accepts variables either one by one
via `-e KEY=VALUE` or via `--env-file FILE`. You can also pass down an existing
environment variable by specifying just `-e KEY` without an assignment. Here is
an example:

```bash
docker run -e VAST_ENDPOINT -e VAST_IMPORT__BATCH_SIZE=42 --env-file .env \
  tenzir/vast start
```

## Build your own VAST image

You can always build your own Docker image in case our prebuilt images don't fit
your use case.

Our official [Dockerfile](https://github.com/tenzir/vast/blob/main/Dockerfile)
offers two starting points: a *development* and *production* layer.

Before building the image, make sure to fetch all submodules:

```bash
git clone --recursive https://github.com/tenzir/vast
cd vast
```

### Build the production image

The production image is optimized for size and security. This is the official
`tenzir/vast` image. From the repository root, build it as follows:

```bash
docker build -t tenzir/vast .
```

### Build the development image

The development image `tenzir/vast-dev` contains all build-time dependencies of
VAST. It runs with a `root` user to allow for building custom images that build
additional VAST plugins. By default, VAST loads all installed plugins in our
images.

Build the development image by specifying it as `--target`:

```bash
docker build -t tenzir/vast-dev --target development .
```
