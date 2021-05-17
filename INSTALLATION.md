VAST Installation Instructions
==============================

Required dependencies:

- A C++17 compiler:
  - GCC >= 8.0
  - Clang >= 8.0
  - Apple Clang >= 9.1
- [CMake](http://www.cmake.org) >= 3.11
- [flatbuffers](https://google.github.io/flatbuffers/) >= 1.11.0
- [Apache Arrow](https://arrow.apache.org/) >= 0.16
  - Apache Arrow support can be explicitly disabled by configuring with
    `--without-arrow`. Note that this changes the default behavior of VAST.

Optional dependencies:

- [libpcap](http://www.tcpdump.org)
- [Doxygen](http://www.doxygen.org)
- [Pandoc](https://github.com/jgm/pandoc)

## Source Build

Building VAST involves the following steps:

1. Clone the repository recursively:
  ```sh
  git clone --recursive https://github.com/tenzir/vast
  ```
2. Configure the build:
  ```sh
  cd vast
  cmake -B build
  ```
3. Build the executable:
  ```sh
  cmake --build build --target all
  ```
4. Run the unit tests to verify everything works as expected:
  ```sh
  ctest --test-dir build
  ```
5. Install VAST system-wide or into your configured prefix:
  ```sh
  # Optionally pass a custom install prefix via `--prefix /path/to/install`
  cmake --install build
  ```
4. Run the integration tests to verify everything works as expected:
  ```sh
  cmake --build build --target integration
  ```

## Nix

Nix expressions to create a localized build environment with dependencies are
available. To use it, run `nix-shell` in the top-level directory.

The same scaffold can also be used to build and install VAST directly into the
Nix Store and add it to the Nix profile. Invoke `nix-env -f default.nix -i`
in the top-level directory to do so.

## OS-specific Guides

### Linux

Installing on Linux currently requires a build from source as described in
the section above.

On `.deb`/`.rpm`/`slackware`-based distributions `checkinstall` can
be used as an alternative for step (5) above to create a proper
binary package for the installation:

    sudo checkinstall --pkgname=vast

#### Docker

There are pre-built images available on [dockerhub.com/tenzir](https://hub.docker.com/repository/docker/tenzir/vast).
Alternatively, you can build the image as follows:

```sh
docker build -t tenzir/vast:<TAG> --build-arg BRANCH=<master|tag>
```

There also exists a build script, that will tag and exported the Docker images
as `tag.gz` files. The script accepts an optional parameter to build a specific
branch or tag.

```sh
./scripts/docker-build <BRANCH>
```

Start VAST in a container and detach it to the background. When you mount a
directory for persistent data, make sure the `tenzir` user inside the container
can write to it.

```sh
mkdir -p /var/lib/vast
docker run -dt --name=vast --rm -p 42000:42000 -v /var/lib/vast:/var/lib/vast tenzir/vast:latest start
```

### macOS

A current version of VAST can be installed using Homebrew.

Install the *current master* from tenzir/tenzir tap:
```sh
brew install --HEAD tenzir/tenzir/vast
```

Install the *latest release* from tenzir/tenzir tap:
```sh
brew install tenzir/tenzir/vast
```

Run VAST server as a launchd service:
```sh
brew services start vast
```

### FreeBSD

Installation on FreeBSD currently requries a source build because a VAST port
does not exist (yet).

#### Service Management

FreeBSD uses the [rc(8)][rc] system of startup scripts during system
initialization and for managing services. VAST ships with an rc script
(similar to the ones in `/etc/rc.d` and `/usr/local/etc/rc.d`) that allows
[service(8)][service] to manage a `vast` process with the `start`, `stop`, and
`restart` options. The rc script uses [daemon(8)][daemon] to manage the `vast`
process.

The following steps install VAST as a persistent service:

1. Copy the rc script to `/usr/local/etc/rc.d`.

   **Note**: this happens automatically when invoking the `install` target,
   e.g., via `cmake --install build`.

2. Add the following line to `/etc/rc.conf`:

        vast_enable="YES"

3. Start the service:

        service vast start

   During the first start, the rc script checks whether a `vast` user and
   group exist already. These are necessary so that [daemon(8)][daemon] can
   start VAST in an unprivileged context.

You can now verify that [daemon(8)][daemon] brought the `vast` process up:

    service vast status

For more fine-grained information about the running VAST instance, you can use
VAST's builtin `status` command:

    vast status


[rc]: https://www.freebsd.org/cgi/man.cgi?query=rc&sektion=8
[service]: https://www.freebsd.org/cgi/man.cgi?query=service&sektion=8
[daemon]: https://www.freebsd.org/cgi/man.cgi?query=daemon&sektion=8
