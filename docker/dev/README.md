# Development Docker resources

This directory is a container for resources common to multiple integrations.

## app.Dockerfile

A helper to build Poetry based Python integrations between VAST and third party
tools.

The build context should be set to the root of the VAST repository. This script
copies both the VAST Python bindings (`python/`) and the directory specified
with the `RELATIVE_APP_DIR` argument to the `/vast` directory of the container,
maintaining their relative structure identical.

Build arguments:
- `VAST_VERSION`: version of the base VAST image
- `VAST_CONTAINER_REGISTRY`: registry url of the base VAST image
- `RELATIVE_APP_DIR`: the path of the Poetry project of this app relative to the
  build context
