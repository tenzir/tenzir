# Docker compose overlays

## Naming Compose files

All `.yaml` file in this directory being Docker Compose configuration files, we
ignore the convention of prefixing them with `docker-compose.`

Their naming follows the structure `{integration}.{feature}.yaml`:
- `integration`: the name of the core service that is being integrated
- `feature`:
  - if unspecified, the file is expected to contain the base configuration of
    the integration (it can usually work in standalone mode)
  - if `vast` is specified (e.g `quarto.vast.yaml`), it means that the file
    contains the configurations to interact with the `vast` service (e.g
    networking)
  - otherwise a keyword indicating what is being enabled in the `integration` on
    top of the base configuration (e.g `vast.volume.yaml` configures volumes for
    the `vast` service to provide persistence)

## The environment files

If a `.env` file is present at the base of the project directory (by default
this directory), Docker Compose automatically loads environment from it. For
each integration, we provide a `{integration}-env.example` file with the
supported configurations (values are defaulted in the Docker Compose files as
well). You can optionnally create a `.env` file in the project directory and
cherry pick the variables you want to customize from the example files.

## Colocation of Compose files

When specifying multiple configuration files with `-f` or the `COMPOSE_FILE`
variable, Docker Compose uses the directory of the first one as project
directory. This makes it hard to correctly define relative paths if these files
sit in different locations. To avoid this issue, we use this common directory
for all Compose files.
