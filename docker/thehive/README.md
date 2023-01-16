# TheHive and Cortex

> **Warning** The TheHive and Cortex integrations are considered experimental
> and subject to change without notice.

This integration brings together VAST and TheHive, supporting the following use
cases:

1. Trigger a historical query to contextualize an observable through a [Cortex
  Analyzer][cortex-analyzers-docs]. This integration spawns an historical query
  directly from the TheHive UI.

2. Forward alerts from VAST to TheHive. We provide a small "app" that loads is
  capable of loading both stored and continuously incoming alerts from a running
  VAST instance.

### The VAST Cortex Analyzer

The analyzer can issue queries for observables with the following types:

- **ip**: match all events that have a corresponding value in any field with the
  `:ip` type.

- **subnet**: match all events that have a value in any field with the `:ip` type
  that belongs to that subnet.

- **hash/domain**: match all events that contain the associated strings.

> **Note** Queries issued to VAST have limit of 30 results. They are displayed
> in plain JSON.

### TheHive app

The app performs both a historical and a continuous query on Suricata alerts.

- For the historical query, the number of events is limited to 100 (this can be
  increased with the `BACKFILL_LIMIT` variable).

- Alerts are de-duplicated before being ingested into TheHive. Only alerts with
  a unique start time / flow id pair are considered.

> **Note** The app is currently only capable of processing Suricata alerts. The
> conversion to TheHive entities is hardcoded. Our [vision][vision-page] is to
> decouple the integrations by creating a unified Fabric capable of abstracting
> away concepts such as alerts. This will be enabled in particular by our
> current work on [pipelines][pipeline-page].

[vision-page]: https://vast.io/docs/about/vision
[pipeline-page]: https://github.com/tenzir/vast/pull/2577

## Configurations

This directory contains the Docker Compose setup to run a preconfigured instance
of TheHive with a VAST [Cortex Analyzer][cortex-analyzers-docs]. This stack can
run with multiple levels of integration with the VAST service:

- `thehive.yaml`: no dependency on the VAST Compose service.

- `thehive.yaml` + `thehive.vast.yaml`: the analyzer can access the VAST Compose
  service.

- `thehive.yaml` + `thehive.vast.yaml` + `thehive.app.yaml`: the app is relaying
  events between VAST and TheHive.

By default, TheHive is exposed on `http://localhost:9000`. We create default
users for both TheHive and Cortex:

- `admin@thehive.local`/`secret`: organization management

- `orgadmin@thehive.local`/`secret`: user and case management within the default
  (Tenzir) organization

These settings can be configured using [environment
variables](../compose/thehive-env.example).

## Standalone (or with VAST running locally)

If you have VAST instance running locally already or you don't plan on using the
Cortex VAST Analyzer, run the default configuration:

```bash
docker compose -f thehive.yaml up --build
```

You can also use the `VAST_ENDPOINT` environment variable to target a remote
VAST server.

## With VAST running as a Compose service

If you want to connect to a VAST server running as a Docker Compose service,
some extra networking settings required. Those are specified in
`thehive.vast.yaml`. For instance, from the `docker/compose` directory run:

```bash
docker compose \
    -f thehive.yaml \
    -f thehive.vast.yaml \
    -f vast.yaml \
    up --build
```

## With the VAST app

We provide a basic integration script that listens on `suricata.alert` events
and forwards them to TheHive. You can start it along the stack by running:

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

You can also test it out on a real-world dataset:
```bash
wget -O - -o /dev/null https://storage.googleapis.com/tenzir-public-data/malware-traffic-analysis.net/2020-eve.json.gz | \
  gzip -d | \
  head -n 1000 | \
  docker compose run --no-TTY vast import --blocking suricata
```

> **Note** To avoid forwarding the same alert multiple times, we use the hash of
> the connection timestamp and the flow id as `sourceRef` of the TheHive alert.
> This fields is guarantied to be unique by TheHive. The app also maintains a
> local cache of all the `sourceRef` values that it already tried to import.

[cortex-analyzers-docs]: https://docs.thehive-project.org/cortex/
