# Development Docker resources

This directory is a container for resources common to multiple integrations.

## Dockerfile

A helper image to build and run Python integrations with VAST without having to
install the Poetry dependencies locally.

The build context should be set to the root of the VAST repository. This script
copies the VAST Python bindings (`python/`) and test data
(`vast/integration/data`) to the `/vast` directory of the container, maintaining
their relative structure identical.

Build arguments:
- `VAST_VERSION`: version of the base VAST image
- `VAST_CONTAINER_REGISTRY`: registry url of the base VAST image
