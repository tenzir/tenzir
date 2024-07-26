# Build the Docker image

Our [Dockerfile](https://github.com/tenzir/tenzir/blob/main/Dockerfile) has two
starting points: a *development* and *production* layer.

Before building the image, make sure to fetch all submodules:

```bash
git clone --recursive https://github.com/tenzir/tenzir
cd tenzir
git submodule update --init --recursive -- libtenzir plugins tenzir
```

## Build the production image

The production image is optimized for size and security. This is the official
`tenzir/tenzir` image. From the repository root, build it as follows:

```bash
docker build -t tenzir/tenzir .
```

## Build the development image

The development image `tenzir/tenzir-dev` contains all build-time dependencies
of Tenzir. It runs with a `root` user to allow for building custom images that
build additional Tenzir plugins. By default, Tenzir loads all installed plugins
in our images.

Build the development image by specifying it as `--target`:

```bash
docker build -t tenzir/tenzir-dev --target development .
```
