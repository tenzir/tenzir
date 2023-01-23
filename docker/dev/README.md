# Development Docker resources

This directory is a container for resources common to multiple integrations.

## Dockerfile

A helper image to build Python integrations on top of VAST without having to
install the Poetry dependencies locally.

The build context should be set to the root of the VAST repository. This script
copies both the VAST Python bindings (`python/`) and the directory specified
with the `RELATIVE_APP_DIR` argument to the `/vast` directory of the container,
maintaining their relative structure identical.

Stages:
- `vast-python-script`: run a VAST Python Poetry script (e.g `pytest`)
- `vast-python-app`: run apps that depend on VAST Python

Build arguments:
- `VAST_VERSION`: version of the base VAST image
- `VAST_CONTAINER_REGISTRY`: registry url of the base VAST image
- `RELATIVE_APP_DIR` (`vast-python-app` only): the path of the dependent (app)
  Poetry project relative to the build context
