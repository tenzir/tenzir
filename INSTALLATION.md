VAST Installation Instructions
==============================

Required dependencies:

- A C++17 compiler:
  - GCC >= 8
  - Clang >= 6
  - Apple Clang >= 9.1
- [CMake](http://www.cmake.org) >= 3.11
- [Arrow](https://arrow.apache.org/) >= 0.15

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

TODO

### macOS

TODO

### FreeBSD

TODO
