---
sidebar_position: 0
---

# Download

You can obtain various artifacts of VAST along the CD pipeline.

## Source Code

Get the source code by cloning our Git repository or downloading an archive.

### Git

Use `git` to clone our repository hosted on GitHub:

```bash
git clone --recursive https://github.com/tenzir/vast
```

You can check out the `stable` branch to get the latest released version:

```bash
cd vast
git checkout stable
```

### Archive

Download a zip Archive of the [latest release][latest-release] or the current
development version:

<div align="center">
  <a class="button button--md button--primary margin-right--md" href="https://github.com/tenzir/vast/archive/refs/heads/stable.zip">Source Code (Release)</a>
  <a class="button button--md button--info margin-left--md" href="https://github.com/tenzir/vast/archive/refs/heads/master.zip">Source Code (Development)</a>
</div>

## Packages

We offer pre-built versions of VAST containing a statically linked binary, for
the [latest release][latest-release-build] and the [current development
version][development-version-build].

### Generic Linux

<div align="center" class="padding-bottom--md">
  <a class="button button--md button--primary margin-right--md" href="https://github.com/tenzir/vast/releases/latest/download/vast-linux-static.tar.gz">Static Build (Release)</a>
  <a class="button button--md button--info margin-left--md" href="https://storage.googleapis.com/tenzir-public-data/vast-static-builds/vast-static-latest.tar.gz">Static Build (Development)</a>
</div>

### Debian

<div align="center" class="padding-bottom--md">
  <a class="button button--md button--primary margin-right--md" href="https://github.com/tenzir/vast/releases/latest/download/vast-linux-static.deb">Static Build (Release)</a>
  <a class="button button--md button--info margin-left--md" href="https://storage.googleapis.com/tenzir-public-data/vast-static-builds/vast-static-latest.deb">Static Build (Development)</a>
</div>

We also offer prebuilt statically linked binaries for every git commit to the
`master` branch.

```bash
https://storage.googleapis.com/tenzir-public-data/vast-static-builds/vast-${version}-linux-static.tar.gz
https://storage.googleapis.com/tenzir-public-data/vast-static-builds/vast_${version}_amd64.deb
```

To determine the version, check out the desired commit locally and run this
command:

```bash
version="$(git describe --abbrev=10 --long --dirty --match='v[0-9]*')"
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

Read our [Docker instructions](/docs/setup-vast/deploy/docker) for more details
on using Docker.

[latest-release]: https://github.com/tenzir/vast/releases/latest
[latest-release-build]: https://github.com/tenzir/vast/releases/latest/download/vast-linux-static.tar.gz
[development-version-build]: https://storage.googleapis.com/tenzir-public-data/vast-static-builds/vast-static-latest.tar.gz
