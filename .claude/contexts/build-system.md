# Build types

There are three main ways to build tenzir, described in more detail below:
The native build, the docker build and the nix build.

If the instructions don't specify the build type, assume a local build
was intended.

For the selected build type, verify that the prerequisites are met.

## Local build

### Prerequisites

1. **CMake** 3.30 or newer installed and available in PATH
2. **C++ Compiler** with C++23 support:
   - GCC 10.0+ or Clang 13.0+ (Apple Clang 14.0+)
3. **Git** with submodules initialized: `git submodule update --init --recursive`
4. **OpenSSL** development libraries (for CAF networking)
5. **Poetry** (for Python bindings, if enabled)

### Build Setup

1. **Check submodules**: Verify all submodules are initialized, especially CAF in `libtenzir/aux/caf/`

2. **Create build directory**:
   - Look for existing build directory named `build/`
   - If none exists, create one: `cmake -B build/ [CMAKE_ARGS]`

3. **Configure the build**:
   ```bash
   cmake -B build/ -DCMAKE_BUILD_TYPE=RelWithDebInfo [OTHER_OPTIONS]
   ```

4. **Build the project**:
   - For Tenzir binary only: `cmake --build build/ --target tenzir`
   - For everything: `cmake --build build/`
   - Always run with unlimited timeout (builds can take 30+ minutes)

### Build Configuration Options

#### Build Types
- `-DCMAKE_BUILD_TYPE=Release` - Optimized release build
- `-DCMAKE_BUILD_TYPE=RelWithDebInfo` - Release with debug info (default)
- `-DCMAKE_BUILD_TYPE=Debug` - Debug build with assertions
- `-DCMAKE_BUILD_TYPE=CI` - CI-specific optimized build

#### Core Options
- `-DBUILD_SHARED_LIBS=ON/OFF` - Build shared vs static libraries (default: ON)
- `-DTENZIR_PLUGINS=plugin1,plugin2` - Build specific plugins only
- `-DTENZIR_ENABLE_BUNDLED_CAF=ON/OFF` - Use bundled CAF vs system CAF
- `-DTENZIR_ENABLE_STATIC_EXECUTABLE=ON/OFF` - Create statically linked binary

#### Development Options
- `-DTENZIR_ENABLE_DEVELOPER_MODE=ON/OFF` - Enable developer features (default: ON for main build)
- `-DTENZIR_ENABLE_UNIT_TESTS=ON/OFF` - Build unit tests (default: ON)
- `-DTENZIR_ENABLE_ASSERTIONS=ON/OFF` - Enable runtime assertions
- `-DTENZIR_ENABLE_ASAN=ON/OFF` - Enable AddressSanitizer (default: ON for Debug/CI)
- `-DTENZIR_ENABLE_CLANG_TIDY=ON/OFF` - Run clang-tidy during build

#### Performance Options
- `-DTENZIR_ENABLE_AUTO_VECTORIZATION=ON/OFF` - Enable SSE/AVX optimizations
- `-DCMAKE_INTERPROCEDURAL_OPTIMIZATION=ON/OFF` - Enable LTO for Release builds

#### Feature Options
- `-DTENZIR_ENABLE_PYTHON_BINDINGS=ON/OFF` - Build Python bindings (default: ON)
- `-DTENZIR_ENABLE_BUNDLED_SCHEMAS=ON/OFF` - Install schema files (default: ON)
- `-DTENZIR_ENABLE_MANPAGES=ON/OFF` - Generate manual pages (default: ON)

### Plugin Configuration

By default, docker is built with all plugins enabled. For development,
it's usually a good idea to only enable the relevant plugins to keep build
times down. The `platform` plugin is required to connect the node to the
Tenzir Platform.

To build specific plugins only:
```bash
cmake -B build/ -DTENZIR_PLUGINS="platform,web,parquet"
```

### Running After Build

- Binary location: `build/tenzir/tenzir`
- Run with: `./build/tenzir/tenzir --help`
- Run integration tests: `uv run tenzir/tests/run.py`

## Docker build

### Prerequisites

  - The `docker` binary is installed and available in the PATH.

### Build Configuration

  - To make a docker build, use `docker build .`
  - The docker build is very resource-intensive, so ensure that a
    resource-constrained builder is available and use that for the build.

```
[worker.oci]
max-parallelism = 2
```

## Nix build

### Prerequisites
 - The `nix` binary must be installed and in the path
 - Flakes must be enabled (most modern Nix installations)

### Instructions

Tenzir provides several Nix build options:

1. **Build specific packages:**
   - `nix build .#tenzir-de` - Development edition (open source only)
   - `nix build .#tenzir` - Full edition (includes closed source plugins)
   - `nix build .#tenzir-static` - Statically linked version (default)
   - `nix build .#tenzir-de-static` - Statically linked development edition

2. **Run without installing:**
   - `nix run .#tenzir-de` - Run the development edition
   - `nix run .#tenzir` - Run the full edition
   - `nix run` - Run the default (tenzir-static)

3. **Enter development shell:**
   - `nix develop` - Enter a development environment with all dependencies

4. **Static binary build script:**
   - Use `nix/static-binary.sh` for custom static builds with additional plugins
   - Supports `--with-plugin=<path>` to include external plugins
   - Supports `-D<CMake option>` to pass CMake flags

The built binaries will be available in `./result/bin/tenzir` after a successful build.
