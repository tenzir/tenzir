# MISP integration

This directory contains the scripts to run MISP as part of the VAST Docker
compose stack.

## MISP app

To run the misp app, you first need to build the VAST Poetry image (we plan to
host this image on Dockerhub to make this step optional):
```
cd docker/poetry
docker compose up
cd ../misp
docker compose -f docker-compose.app.yaml run misp-app
```

## Dynamic hostname

The `proxy` directory contains the setup for an NGINX that automatically
translates the MISP hostname to the one provided in the `Host` HTTP header of
the incomming requests. This is useful when running MISP behind some sort
of reverse proxy. You can use this proxy by applying the
`docker-compose.proxy.yaml` overlay.
