# Tenzir Platform CLI

This package distributes the compiled Tenzir Platform CLI for Linux x86_64 and
ARM64, and macOS 13 or later on ARM64. Install it with
`pip install tenzir-platform-cli`, then run
`platform-cli --help`.

The `tenzir` package installs this dependency automatically and exposes
it through `tenzir platform`. With `TENZIR_UNIFIED=1`, platform commands are also
available directly through `tenzir`.

Build a wheel from a repository checkout with
`uv build --wheel --package tenzir-platform-cli` in `engine/python/`. The build
uses the engine flake's `platform-cli` output. To package an existing binary, set
`TENZIR_PLATFORM_CLI_PATH` to its absolute path. Set `TENZIR_WHEEL_PLATFORM` when
packaging a binary for a different architecture.

macOS releases use `TENZIR_SIGNED_TARBALL_PATH` to extract the signed and
notarized executable from the native package without changing its bytes.
