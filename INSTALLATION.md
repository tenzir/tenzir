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
   e.g., via `cmake --build build --target install`.

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
