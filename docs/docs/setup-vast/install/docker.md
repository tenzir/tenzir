# Docker

:::tip Docker Hub
We provide pre-built Docker images at [dockerhub.com/tenzir][dockerhub].
:::

[dockerhub]: https://hub.docker.com/repository/docker/tenzir/vast

Our Docker image contains a dynamic of VAST build with plugins as shared
libraries. The system user `vast` runs the VAST executable with limited
privileges. Database contents go into the volume exposed at `/var/lib/vast`.

## Start the container

Start VAST in a container and detach it to the background.

```bash
mkdir -p /var/lib/vast
docker run -dt --name=vast --rm -p 42000:42000 -v /var/lib/vast:/var/lib/vast \
  tenzir/vast:latest start
```

## Build the main image

Build the `tenzir/vast` image as follows:

```bash
docker build -t tenzir/vast:<TAG>
```

## Build the development image

In addition to the `tenzir/vast` image, the development image `tenzir/vast-dev`
contains all build-time dependencies of VAST. It runs with a `root` user to
allow for building custom images that build additional VAST plugins. VAST in the
Docker images is configured to load all installed plugins by default.

You can build the development image as follows:

```bash
docker build -t tenzir/vast-dev:<TAG> --target development
```
