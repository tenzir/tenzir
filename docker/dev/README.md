# Development Docker resources

This directory is a container for resources common to multiple integrations.

## Dockerfile

A helper image to build and run Python integrations with Tenzir without having to
install the Poetry dependencies locally.

The build context should be set to the root of the Tenzir repository. This script
copies the Tenzir Python bindings (`python/`) and test data
(`vast/integration/data`) to the `/vast` directory of the container, maintaining
their relative structure identical.

Build arguments:
- `TENZIR_VERSION`: version of the base Tenzir image
- `TENZIR_CONTAINER_REGISTRY`: registry url of the base Tenzir image
