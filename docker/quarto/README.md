# Quarto runner

In this directory we define a dockerized environment to run Quarto. The image
builds on top of the VAST image and adds R, Poetry, and Quarto. This creates a
runtime suitable for calling the VAST binary from notebooks.

We don't publish the Quarto Docker image, so you need to build it locally.

The `docker-compose.bind.yaml` overlay contains the configurations to bind mount
the VAST repository. The provided commands are executed as the `vast:vast` user
but the resulting output files will be created from the user defined in the
`HOST_UID` and `HOST_GID` environment variables. From this directory:

```bash
# required to allow writing on the host without permission conflicts
export HOST_UID=$(id -u) 
export HOST_GID=$(id -g) 

docker compose \
    -f docker-compose.yaml \
    -f docker-compose.bind.yaml \
    run quarto \
    make -C web
```

The `docker-compose.vast.yaml` overlay adds the settings to enable communication
between the VAST binary in the Quarto image and the `vast` service (configured
using the `vast` Compose configurations). From this directory:

```
export HOST_UID=$(id -u) 
export HOST_GID=$(id -g) 

OVERLAYS="-f ../vast/docker-compose.yaml \
    -f docker-compose.yaml \
    -f docker-compose.bind.yaml \
    -f docker-compose.vast.yaml"

docker compose \
    $OVERLAYS \
    up -d vast

docker compose \
    $OVERLAYS \
    run quarto \
    make -C examples/notebooks

docker compose \
    $OVERLAYS \
    down
```
