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
git clone --recursive git@github.com/tenzir/vast.git
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

We offer pre-built versions of VAST containing a statically linked binary, for the
[latest release][latest-release] and the current development version.

<div align="center" class="padding-bottom--md">
  <a class="button button--md button--primary" href="https://github.com/tenzir/vast/releases/latest/download/vast-linux-static.tar.gz">Static Build (Release)</a>
</div>

For development versions or a specific Git commit, you need to navigate through
the GitHub CI to find the build artifact:

1. Browse to the [VAST static workflow][vast-workflow]
2. Click on the latest run, e.g., `Merge pull request...`
3. Scroll to the end of the page
4. Click on the artifact filename, e.g.,
   `vast-v1.0.0-101-g6e7a4ef1a4-linux-static.tar.gz`

[vast-workflow]: https://github.com/tenzir/vast/actions?query=branch%3Amaster+workflow%3A%22VAST%22
## Images

Our CI builds Docker images for the latest release and the current development
version.

### Docker

You can download pre-built Docker images from
[Docker Hub](https://hub.docker.com/repository/docker/tenzir/vast).

Read our [Docker instructions](/docs/setup-vast/deploy/docker) for more details
on using Docker.

[latest-release]: https://github.com/tenzir/vast/releases/latest
