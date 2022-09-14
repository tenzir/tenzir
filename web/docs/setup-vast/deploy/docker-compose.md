---
sidebar_position: 1
---

# Docker-Compose

Our [docker-compose file](../../../../docker-compose.yml) provides an easy way to deploy VAST.

## Run without building
```bash
docker-compose up -d
```

## Build image and run VAST
```bash
git clone --recursive https://github.com/tenzir/vast
cd vast
docker build -t tenzir/vast .
docker-compose up -d
```
