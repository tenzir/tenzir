# TheHive and Cortex

This directory contains the Docker Compose setup to run a preconfigured instance
of TheHive with a VAST [Cortex Analyzer](https://docs.thehive-project.org/cortex/).

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
    -f ../vast/docker-compose.yaml \
    -f docker-compose.yaml \
    -f docker-compose.vast.yaml \
    up --build
```
