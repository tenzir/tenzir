---
sidebar_position: 0
---

# Download

We provide various downloadable artifacts of VAST as output of our CD pipeline.

## Packages

Our pre-built VAST packages contain a statically linked binary, for the latest
release and the current development version.

### Generic Linux

<div align="center" class="padding-bottom--md">
  <a class="button button--md button--primary margin-right--md" href="https://github.com/tenzir/vast/releases/latest/download/vast-linux-static.tar.gz">Static Build (Release)</a>
  <a class="button button--md button--info margin-left--md" href="https://storage.googleapis.com/tenzir-public-data/vast-static-builds/vast-linux-static-latest.tar.gz">Static Build (Development)</a>
</div>

### Debian

<div align="center" class="padding-bottom--md">
  <a class="button button--md button--primary margin-right--md" href="https://github.com/tenzir/vast/releases/latest/download/vast-linux-static.deb">Static Build (Release)</a>
  <a class="button button--md button--info margin-left--md" href="https://storage.googleapis.com/tenzir-public-data/vast-static-builds/vast-linux-static-latest.deb">Static Build (Development)</a>
</div>

We also offer prebuilt statically linked binaries for every Git commit to the
`main` branch.

```bash
version="$(git describe --abbrev=10 --long --dirty --match='v[0-9]*')"
curl -fsSL "https://storage.googleapis.com/tenzir-public-data/vast-static-builds/vast-${version}-linux-static.tar.gz"
```

### Nix

Our repository contains a `flake.nix` that provides a VAST as an app, you can
use `vast = "github:tenzir/vast/stable"` as an input in your own flake or just
try it out with:

```bash
nix run github:tenzir/vast/stable
```

## Images

Our CI builds Docker images for the latest release and the current development
version.

### Docker

You can download pre-built Docker images from
[Docker Hub](https://hub.docker.com/repository/docker/tenzir/vast).

Read our [Docker instructions](deploy/docker.md) for more details on using Docker.

## Source Code

Get the source code by cloning our Git repository or downloading an archive.

### Git

Use `git` to clone our repository hosted on GitHub:

```bash
git clone --recursive https://github.com/tenzir/vast
```

Check out the `stable` branch to get the latest released version:

```bash
cd vast
git checkout stable
```

### Archive

Download a zip Archive of the [latest release][latest-release] or the current
development version:

<div align="center">
  <a class="button button--md button--primary margin-right--md" href="https://github.com/tenzir/vast/archive/refs/heads/stable.zip">Source Code (Release)</a>
  <a class="button button--md button--info margin-left--md" href="https://github.com/tenzir/vast/archive/refs/heads/main.zip">Source Code (Development)</a>
</div>

[latest-release]: https://github.com/tenzir/vast/releases/latest
