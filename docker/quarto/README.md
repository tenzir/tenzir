# Quarto runner

In this directory we define a dockerized environment to run Quarto.

We don't publish the Quarto Docker image, so users are required to build it
locally.

The `docker-compose.bind.yaml` overlay contains the configurations to bind mount
the VAST repository. The provided commands are executed as the `vast:vast` user
but the resulting output files will be created from the user defined in the
`HOST_UID` and `HOST_GID` environment variables.

```bash
# required to allow writing on the host without permission conflicts
export HOST_UID=$(id -u) 
export HOST_GID=$(id -g) 

docker compose \
    -f docker-compose.yaml \
    -f docker-compose.bind.yaml \
    run quarto \
    make -C web

# OR

docker compose \
    -f docker-compose.yaml \
    -f docker-compose.bind.yaml \
    run quarto \
    make -C examples/notebooks
```
