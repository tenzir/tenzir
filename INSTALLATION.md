VAST Installation Instructions
==============================

Required dependencies:

- A C++17 compiler:
  - GCC >= 8
  - Clang >= 6
  - Apple Clang >= 9.1
- [CMake](http://www.cmake.org) >= 3.11
- [Apache Arrow](https://arrow.apache.org/) >= 0.15
  - Apache Arrow support can be explicitly disabled by configuring with
    `--without-arrow`. Note that this changes the default behavior of VAST.

Optional dependencies:

- [libpcap](http://www.tcpdump.org)
- [gperftools](http://code.google.com/p/google-perftools)
- [Doxygen](http://www.doxygen.org)
- [Pandoc](https://github.com/jgm/pandoc)

## Source Build

Building VAST involves the following steps:

1. Clone the repository recursively:
  ```sh
  git clone --recursive https://github.com/tenzir/vast
  ```
2. Configure the build, either with our `configure` wrapper script or manually
   with CMake:
  ```sh
  cd vast
  ./configure
  ```
3. Build the executable:
  ```sh
  cmake --build build --target all
  ```
4. Run the tests to verify everything works as expected:
  ```sh
  cmake --build build --target test
  cmake --build build --target integration
  ```
5. Install VAST system-wide or into your configured prefix:
  ```sh
  cmake --build build --target install
  ```

## OS-specific Guides

### Linux

TODO

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
mkdir -p /var/db/vast
docker run -dt --name=vast --rm -p 42000:42000 -v /var/db/vast:/data tenzir/vast:latest start
```

### macOS

TODO

### FreeBSD

TODO
