# TheHive and Cortex

> **Warning** The TheHive and Cortex integrations are considered experimental
> and subject to change without notice.

This directory contains the Docker Compose setup to run a preconfigured instance
of TheHive with a VAST [Cortex Analyzer][cortex-analyzers-docs]. This stack can
run with multiple levels of integration with the VAST service:
- `thehive.yaml`: no dependency on the VAST Compose service
- `thehive.yaml` + `thehive.vast.yaml`: the analyzer can access the VAST Compose
  service
- `thehive.yaml` + `thehive.vast.yaml` + `thehive.app.yaml`: the app is relaying
  events between VAST and TheHive

By default, TheHive is exposed on `http://localhost:9000`. We create some
default users for both TheHive and Cortex:
- `admin@thehive.local`/`secret`: organization management
- `orgadmin@thehive.local`/`secret`: user and case management within the default
  (Tenzir) organization

These settings can be configured using [environment variables](env.example).

[cortex-analyzers-docs]: https://docs.thehive-project.org/cortex/

## Standalone (or with VAST running locally)

If you have VAST instance running locally already or you don't plan on using the
Cortex VAST Analyzer, run the default configuration:

```bash
docker compose up -f thehive.yaml --build
```

You can also use the `VAST_ENDPOINT` environment variable to target a remote
VAST server.

## With VAST running as a Compose service

If you want to connect to a VAST server running as a Docker Compose service,
some extra networking settings required. Those are specified in
`thehive.vast.yaml`. For instance from the `docker/compose` directory run:

```bash
docker compose \
    -f thehive.yaml \
    -f thehive.vast.yaml \
    -f vast.yaml \
    up --build
```

## With the VAST app

We provide a very basic integration script that listens on `suricata.alert`
events and forwards them to TheHive. You can start it along the stack by
running:
```bash
# from the docker/compose directory
export COMPOSE_FILE="vast.yaml"
COMPOSE_FILE="$COMPOSE_FILE:thehive.yaml"
COMPOSE_FILE="$COMPOSE_FILE:thehive.vast.yaml"
COMPOSE_FILE="$COMPOSE_FILE:thehive.app.yaml"

docker compose up --build --detach
```

To test the alert forwarding with some mock data, run:
```bash
# from the docker/compose directory with the COMPOSE_FILE variable above
docker compose run --no-TTY vast import --blocking suricata \
    < ../../vast/integration/data/suricata/eve.json
```

Note: To avoid forwarding the same alert multiple times, we use the hash of the
connection timestamp and the flow id as `sourceRef` of the TheHive alert.
