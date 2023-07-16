# Quarto runner

In this directory we define a dockerized environment to run Quarto. The image
builds on top of the [Tenzir image](../../Dockerfile) and adds R, Poetry, and
Quarto. This creates a runtime suitable for calling the Tenzir binary from
notebooks.

We don't publish the Quarto Docker image, so you need to build it locally.

The `quarto.bind.yaml` overlay contains the configurations to bind-mount
the Tenzir repository on the Quarto container and avoid filesystem permission
conflicts between the container and the host users. For instance, to run the
Quarto build in the `web` directory, open a terminal in the `docker/compose`
directory and run:

```bash
# required to allow writing on the host without permission conflicts
export HOST_UID=$(id -u) 
export HOST_GID=$(id -g) 

docker compose \
    -f quarto.yaml \
    -f quarto.bind.yaml \
    run quarto \
    make -C web
```

The `quarto.tenzir.yaml` overlay adds the settings to enable communication
between the Tenzir binary in the Quarto image and the `tenzir` service (configured
using the `tenzir` Compose configurations). To perform the Quarto build of the
examples notebooks with a pristine `tenzir` service running in the background, in
the `docker/compose` directory run:

```bash
export HOST_UID=$(id -u) 
export HOST_GID=$(id -g) 

export COMPOSE_FILE="tenzir.yaml"
COMPOSE_FILE="$COMPOSE_FILE:quarto.yaml"
COMPOSE_FILE="$COMPOSE_FILE:quarto.bind.yaml"
COMPOSE_FILE="$COMPOSE_FILE:quarto.tenzir.yaml"

docker compose up -d tenzir

docker compose \
    run quarto \
    make -C examples/notebooks

docker compose down
```
