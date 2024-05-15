---
sidebar_position: 0
---

# Build from source

Tenzir uses [CMake](https://cmake.org) as build system. Aside from a modern C++20
compiler, you need to ensure availability of the dependencies in the table
below.

:::tip Deterministic Builds via Nix
We provide a Nix flake to setup an environment in which all dependencies are
available. Run `nix develop` inside the main source directory. You can also
delegate the entire build process to Nix by invoking `nix build`, but be aware
that this method does not support incremental
builds.
:::

## Dependencies

Every [release](https://github.com/tenzir/tenzir/releases) of Tenzir includes an
[SBOM](https://en.wikipedia.org/wiki/Software_bill_of_materials) in
[SPDX](https://spdx.dev) format that lists all dependencies and their versions.

<div align="center" class="padding-bottom--md">
  <a class="button button--md button--primary margin-right--md" href="https://github.com/tenzir/tenzir/releases/latest/download/tenzir.spdx.json">Latest SBOM</a>
</div>

|Required|Dependency|Version|Description|
|:-:|:-:|:-:|-|
|✓|C++ Compiler|C++20 required|Tenzir is tested to compile with GCC >= 12.0 and Clang >= 15.0.|
|✓|[CMake](https://cmake.org)|>= 3.19|Cross-platform tool for building, testing and packaging software.|
|✓|[CAF](https://github.com/actor-framework/actor-framework)|>= 0.18.7|Implementation of the actor model in C++. (Bundled as submodule.)|
|✓|[OpenSSL](https://www.openssl.org)||Utilities for secure networking and cryptography.|
|✓|[FlatBuffers](https://google.github.io/flatbuffers/)|>= 2.0.8|Memory-efficient cross-platform serialization library.|
|✓|[Boost](https://www.boost.org)|>= 1.81.0|Required as a general utility library.|
|✓|[Apache Arrow](https://arrow.apache.org)|>= 13.0.0|Required for in-memory data representation. Must be built with Compute, Filesystem, S3, Zstd and Parquet enabled. For the `gcs` plugin, GCS needs to be enabled. |
|✓|[re2](https://github.com/google/re2)||Required for regular expressione evaluation.|
|✓|[yaml-cpp](https://github.com/jbeder/yaml-cpp)|>= 0.6.2|Required for reading YAML configuration files.|
|✓|[simdjson](https://github.com/simdjson/simdjson)|>= 3.1.0|Required for high-performance JSON parsing. (Bundled as submodule.)|
|✓|[spdlog](https://github.com/gabime/spdlog)|>= 1.5|Required for logging.|
|✓|[fmt](https://fmt.dev)|>= 8.1.1|Required for formatted text output.|
|✓|[xxHash](https://github.com/Cyan4973/xxHash)|>= 0.8.0|Required for computing fast hash digests.|
|✓|[robin-map](https://github.com/Tessil/robin-map)|>= 0.6.3|Fast hash map and hash set using robin hood hashing. (Bundled as subtree.)|
|✓|[fast_float](https://github.com/FastFloat/fast_float)|>= 3.2.0|Required for parsing floating point numbers. (Bundled as submodule.)|
|✓|[libmaxminddb](https://github.com/maxmind/libmaxminddb)|>= 1.8.0|Required for the `geoip` context.|
||[libpcap](https://www.tcpdump.org)||Required for building the `pcap` plugin.|
||[librdkafka](https://github.com/confluentinc/librdkafka)||Required for building the `kafka` plugin.|
||[http-parser](https://github.com/nodejs/http-parser)||Required for building the `web` plugin.|
||[cppzmq](https://github.com/zeromq/cppzmq)||Required for building the `zmq` plugin.|
||[pfs](https://github.com/dtrugman/pfs)||Required for the `processes` and `sockets` operators on Linux.|
||[Protocol Buffers](https://protobuf.dev)|>= 1.4.1|Required for building the `velociraptor` plugin.|
||[gRPC](https://grpci.io)|>= 1.51|Required for building the `velociraptor` plugin.|
||[rabbitmq-c](https://github.com/alanxz/rabbitmq-c)||Required for building the `rabbitmq` plugin.|
||[yara](https://yara.readthedocs.io/)|>= 4.4.0|Required for building the `yara` plugin.|
||[poetry](https://python-poetry.org)||Required for building the Python bindings.|
||[Doxygen](http://www.doxygen.org)||Required to build documentation for libtenzir.|
||[Pandoc](https://github.com/jgm/pandoc)||Required to build the manpage for Tenzir.|
||[bash](https://www.gnu.org/software/bash/)|>= 4.0.0|Required to run the integration tests.|
||[bats](https://bats-core.readthedocs.io)|>= 1.8.0|Required to run the integration tests.|

The minimum specified versions reflect those versions that we use in CI and
manual testing. Older versions may still work in select cases.

:::tip macOS
On macOS, we recommend using Clang from the Homebrew `llvm@17` package with the
following settings:

```bash
export LLVM_PREFIX="$(brew --prefix llvm@17)"
export PATH="${LLVM_PREFIX}/bin:${PATH}"
export CC="${LLVM_PREFIX}/bin/clang"
export CXX="${LLVM_PREFIX}/bin/clang++"
export LDFLAGS="-Wl,-rpath,${LLVM_PREFIX} ${LDFLAGS}"
export CPPFLAGS="-isystem ${LLVM_PREFIX}/include ${CPPFLAGS}"
export CXXFLAGS="-isystem ${LLVM_PREFIX}/include/c++/v1 ${CXXFLAGS}"
```

Installing via CMake on macOS configures a [launchd](https://www.launchd.info)
agent to `~/Library/LaunchAgents/com.tenzir.tenzir.plist`. Use `launchctl` to
spawn a node at login:

```bash
# To unload the agent, replace 'load' with 'unload'.
launchctl load -w ~/Library/LaunchAgents/com.tenzir.tenzir.plist
```
:::

## Compile

Building Tenzir involves the following steps:

Clone the repository recursively:

```bash
git clone https://github.com/tenzir/tenzir
cd tenzir
git submodule update --init --recursive -- libtenzir plugins
```

Configure the build with CMake. For faster builds, we recommend passing
`-G Ninja` to `cmake`.

```bash
cmake -B build
# CMake defaults to a "Debug" build. When performance matters, use "Release"
cmake -B build -DCMAKE_BUILD_TYPE=Release  
```

Optionally, you can use the CMake TUI to visually configure the build:

```bash
ccmake build
```

The source tree also contains a set of CMake presets that combine various
configuration options into curated build flavors. You can list them with:

```bash
cmake --list-presets
```

Build the executable:

```bash
cmake --build build --target all
```

## Test

After you have built the executable, run the unit and integration tests to
verify that your build works as expected:

Run component-level unit tests:

```bash
ctest --test-dir build
```

Run the "black box" integration tests:

```bash
cmake --build build --target integration
```

Run end-to-end integration tests:

```bash
cmake --build build --target integration
```

## Install

Install Tenzir system-wide:

```bash
cmake --install build
```

If you prefer to install into a custom install prefix, install with `--prefix
/path/to/install/prefix`.

To remove debug symbols from the installed binaries and libraries, pass
`--strip`.

To install only files relevant for running Tenzir and not for plugin development
pass `--component Runtime`.

## Clean

In case you want to make changes to your build environment, we recommend
deleting the build tree entirely:

```bash
rm -rf build
```

This avoids subtle configuration glitches of transitive dependencies. For
example, CMake doesn't disable assertions when switching from a `Debug` to
a `Release` build, but would do so when starting with a fresh build of type
`Release`.
