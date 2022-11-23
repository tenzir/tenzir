# MISP integration

This directory contains the scripts to run MISP as part of the VAST Docker
compose stack.

## MISP app

Run:
```
COMPOSE_FILE="docker-compose.yaml"
COMPOSE_FILE="$COMPOSE_FILE:docker-compose.app.yaml"
export COMPOSE_FILE="$COMPOSE_FILE:../vast/docker-compose.yaml"

docker compose up --build
```

## Dynamic hostname

The `proxy` directory contains the setup for an NGINX that automatically
translates the MISP hostname to the one provided in the `Host` HTTP header of
the incomming requests. This is useful when running MISP behind some sort
of reverse proxy. You can use this proxy by applying the
`docker-compose.proxy.yaml` overlay.
