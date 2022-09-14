---
sidebar_position: 1
---

# Docker-Compose

Our [docker-compose file](../../../../docker-compose.yml) provides an easy way to deploy VAST.

Default behaviour of docker-compose file is to build the image.

## Build and run VAST
```bash
git clone --recursive https://github.com/tenzir/vast
cd vast
docker-compose up -d
```

## Run without building
```bash
docker compose up --no-build -d
```
