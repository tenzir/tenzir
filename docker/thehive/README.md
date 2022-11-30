# TheHive and Cortex

> **Warning** The TheHive and Cortex integrations are considered experimental
> and subject to change without notice.

This directory contains the Docker Compose setup to run a preconfigured instance
of TheHive with a VAST [Cortex Analyzer][cortex-analyzers-docs]. This stack can
run with multiple levels of integration with the VAST service:
- `docker-compose.yaml`: no dependency on the VAST Compose service
- `docker-compose.yaml` + `docker-compose.vast.yaml`: the analyzer can access
  the VAST Compose service
- `docker-compose.yaml` + `docker-compose.vast.yaml` +
  `docker-compose.app.yaml`: the app is relaying events between VAST and TheHive

By default, TheHive is exposed on `http://localhost:9000`. We create some
default users for both TheHive and Cortex:
- `admin@thehive.local`/`secret`: organization management
- `orgadmin@thehive.local`/`secret`: user and case management within the default
  (Tenzir) organization

These settings can be configured using [environment variables](env.example).

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

You can also test it out on a real world dataset:
```bash
wget -O - -o /dev/null https://storage.googleapis.com/tenzir-public-data/malware-traffic-analysis.net/2020-eve.json.gz | \
  gzip -d | \
  head -n 1000 | \
  docker compose run --no-TTY vast import --blocking suricata
```

Note: To avoid forwarding the same alert multiple times, we use the hash of the
connection timestamp and the flow id as `sourceRef` of the TheHive alert.
