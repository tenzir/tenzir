# TheHive and Cortex

This directory contains the Docker Compose setup to run a preconfigured instance
of TheHive  a VAST [Cortex Analyzer](https://docs.thehive-project.org/cortex/).

## With VAST running on host

```bash
docker compose \
    -f docker-compose.yaml \
    up --build
```

## With VAST running as a Compose service

```bash
docker compose \
    -f ../vast/docker-compose.yaml \
    -f docker-compose.yaml \
    -f docker-compose.vast.yaml \
    up --build
```
