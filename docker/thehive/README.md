# TheHive and Cortex

This directory contains the Docker Compose setup to run a preconfigured instance
of TheHive with a VAST [Cortex Analyzer][cortex-analyzers-docs]. This stack can
run with multiple levels of integration with the VAST service.

By default, TheHive is exposed on `http://localhost:9000`. We create some
default users for both TheHive and Cortex:
- `admin@thehive.local`/`secret`: organization management
- `orgadmin@thehive.local`/`secret`: user and case management within the default
  (Tenzir) organization

[cortex-analyzers-docs]: https://docs.thehive-project.org/cortex/

## With VAST running on localhost

If you have VAST instance running locally already, run the default configuration:

```bash
docker compose up --build
```

You can also use the `VAST_ENDPOINT` environment variable to target a remote
VAST server.

## With VAST running as a Compose service

If you want to connect to a VAST server running as a Docker Compose service,
some extra networking settings required. Those are specified in
`docker-compose.vast.yaml`. For instance you can run:

```bash
docker compose \
    -f docker-compose.yaml \
    -f docker-compose.vast.yaml \
    -f ../vast/docker-compose.yaml \
    up --build
```

## With the VAST app

The integration uses an extended image of VAST with Poetry installed called
`tenzir/vast-poetry`. We plan to publish that image, but for now you need to
build it locally:
```bash
# from the docker/poetry directory
docker compose build
```

We provide a very basic integration script that listens on `suricata.alert`
events and forwards them to TheHive. You can start it along the stack by
running:
```bash
# from the docker/thehive directory
# the COMPOSE_FILE variable is automatically picked up by Compose
COMPOSE_FILE="docker-compose.yaml"
COMPOSE_FILE="$COMPOSE_FILE:docker-compose.vast.yaml"
COMPOSE_FILE="$COMPOSE_FILE:docker-compose.app.yaml"
export COMPOSE_FILE="$COMPOSE_FILE:../vast/docker-compose.yaml"

docker compose up --build -d
```

To test the alert forwarding with some mock data, run:
```bash
# from the docker/thehive directory with the COMPOSE_FILE variable above
docker compose run --no-TTY vast import --blocking suricata \
    < ../../vast/integration/data/suricata/eve.json
```
