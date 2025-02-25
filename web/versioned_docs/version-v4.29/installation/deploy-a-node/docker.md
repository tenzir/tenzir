# Docker

## Install and run a node

We provide Docker images and a Docker Compose file for a container setup.

After [provisioning a node](README.md), proceed as follows:

1. Select the Docker tab and click the download button to obtain the
   `docker-compose.NODE.yaml` configuration file, where `NODE` is the name you
   entered for your node.
1. Run
   ```bash
   docker compose -f docker-compose.NODE.yaml up --detach
   ```

Edit the Docker Compose file and change [environment
variables](../../configuration.md#environment-variables) to adjust your
configuration.

## Stop a node

Stop a node via the `down` command:

```bash
docker compose -f docker-compose.NODE.yaml down
```

Stop a node and delete its persistent state by adding `--volumes`:

```bash
docker compose -f docker-compose.NODE.yaml down --volumes
```

## Update a node

Run the following commands to update a Docker Compose deployment with a
configuration file `docker-compose.NODE.yaml`:

```bash
docker compose -f docker-compose.NODE.yaml pull
docker compose -f docker-compose.NODE.yaml down
docker compose -f docker-compose.NODE.yaml up --detach
```

Note that we `pull` first so that the subsequent downtime between `down` and
`up` is minimal.
