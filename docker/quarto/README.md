# Quarto runner

In this directory we define a dockerized environment to run Quarto. The image
builds on top of the [VAST image](../../Dockerfile) and adds R, Poetry, and
Quarto. This creates a runtime suitable for calling the VAST binary from
notebooks.

We don't publish the Quarto Docker image, so you need to build it locally.

The `docker-compose.bind.yaml` overlay contains the configurations to bind mount
the VAST repository on the Quarto container and avoid file system permission
conflicts between the container and the host users. For instance, to run the
Quarto build in the `web` directory, open a terminal in the directory containing
this README and run the following:

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
using the `vast` Compose configurations). To run the Quarto build of the
examples notebooks with a pristine `vast` service running in the background, in
the directory containing this README execute:

```bash
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
