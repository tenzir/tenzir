---
sidebar_position: 5
---

# Deploy the platform

The **Tenzir Platform** is the control plane that manages Tenzir Nodes. The
platform also provides a web interface to explore data, create pipelines, and
build dashboards.

:::warning Sovereign Edition Required
Tenzir offers a free and cloud-hosted version of the Tenzir Platform at
[app.tenzir.com](https://app.tenzir.com). This guide explains how to run the
platform on your own premises using the [Sovereign
Edition](https://tenzir.com/pricing).
:::

## Download the Platform

Start with downloading the [latest Tenzir Platform
release](https://github.com/tenzir/platform/releases/latest) and unpack the
archive.

## Set Up Docker Registry Access

As part of your distribution, you were provided an authentication token
(`YOUR_DOCKER_TOKEN` below) to fetch the Docker images. Log in with the token
as follows:

```bash
echo YOUR_DOCKER_TOKEN | docker login ghcr.io -u tenzir-distribution --password-stdin
```

## Configure the Platform

You need to configure a few external services to run the platform, such as a
HTTP reverse proxy, an identity provider, and a state database.

Scroll down to the [configuration options](#configuration-options) and populate
a `.env` file with your individual settings.

## Run the Platform

After you configured all services, choose a pre-canned deployment template from
the `examples` directory:

```text {0} title="❯ tree examples"
.
├── localdev
│   ├── docker-compose.yaml
│   └── env.example
└── onprem
    ├── docker-compose.yaml
    └── env.example
```

Change into one of these directories and start the platform in the foreground
with

```bash
docker compose up
```

or `docker compose up --detach` to run it in the background.

```text {0} title="❯ docker compose up"
[+] Running 5/5
 ✔ Container compose-app-1                Running
 ✔ Container compose-websocket-gateway-1  Running
 ✔ Container compose-seaweed-1            Running
 ✔ Container compose-platform-1           Running
Attaching to app-1, platform-1, postgres-1, seaweed-1, websocket-gateway-1
platform-1           | {"event": "connecting to postgres", "level": "debug", "ts": "2024-04-10T10:13:20.205616Z"}
platform-1           | {"event": "connected to postgres", "level": "debug", "ts": "2024-04-10T10:13:20.210667Z"}
platform-1           | {"event": "created table", "level": "info", "ts": "2024-04-10T10:13:20.210883Z"}
platform-1           | {"event": "connecting to postgres", "level": "debug", "ts": "2024-04-10T10:13:20.217700Z"}
platform-1           | {"event": "connected to postgres", "level": "debug", "ts": "2024-04-10T10:13:20.221194Z"}
platform-1           | {"event": "creating a table", "level": "info", "ts": "2024-04-10T10:13:20.221248Z"}
platform-1           | {"event": "connecting to postgres", "level": "debug", "ts": "2024-04-10T10:13:20.221464Z"}
platform-1           | {"event": "connected to postgres", "level": "debug", "ts": "2024-04-10T10:13:20.224226Z"}
app-1                | Listening on 0.0.0.0:3000
websocket-gateway-1  | {"event": "connecting to postgres", "level": "debug", "ts": "2024-04-10T10:15:37.033632Z"}
websocket-gateway-1  | {"event": "connected to postgres", "level": "debug", "ts": "2024-04-10T10:15:37.038510Z"}
websocket-gateway-1  | {"event": "created table", "level": "info", "ts": "2024-04-10T10:15:37.042555Z"}
websocket-gateway-1  | {"host": "0.0.0.0", "port": 5000, "common_env": {"base_path": "", "tenzir_proxy_timeout": 60.0}, "local_env": {"store": {"postgres_uri": "postgresql://postgres:postgres@postgres:5432/platform"}, "tenant_manager_app_api_key": "d3d185cc4d9a1bde0e07e24c2eb0bfe9d2726acb3a386f8882113727ac6e90cf", "tenant_manager_tenant_token_encryption_key": "CBOXE4x37RKRLHyUNKeAsfg8Tbejm2N251aKnBXakpU="}, "event": "HTTP server running", "level": "info", "ts": "2024-04-10T10:15:37.045244Z"}
...
```

It may take up to a minute for all services to be fully available.

## Manage the Platform

We provide a command-line utility that simplifies managing users, organizations,
workspaces, and nodes.

Install the [`tenzir-platform`](https://pypi.org/project/tenzir-platform/)
package from PyPI.

You must provide the following environment variables for interacting with the
platform through the CLI:

```bash
TENZIR_PLATFORM_CLI_API_ENDPOINT=api.platform.example.org:5000
TENZIR_PLATFORM_CLI_OIDC_ISSUER_URL=YOUR_OIDC_ISSUER_URL
TENZIR_PLATFORM_CLI_OIDC_CLIENT_ID=YOUR_OIDC_CLIENT_ID
TENZIR_PLATFORM_CLI_OIDC_AUDIENCE=YOUR_OIDC_AUDIENCE
```

Read our documentation on the [Tenzir Platform CLI](../platform-cli.md) to learn
more about managing your platform deployment.

## Update the Platform

To update to the latest platform version, pull the latest images:

```bash
docker compose pull
```

## Configuration Options

The platform requires some external services that must be installed and
configured separately by setting several environment variables described below.

### HTTP Reverse Proxy

The platform uses four URLs that require a HTTP reverse proxy. These URLs may be
mapped to the same or different hostnames.

1. The URL that the user's browser connects to, e.g.,
   `app.platform.example.org`. This serves a web frontend where the user can
   interact with the platform.
2. The URL that the nodes connect to, e.g., `nodes.platform.example.org`. Tenzir
   Nodes connect to this URL to establish long-running WebSocket connections.
3. The URL that the platform's S3-compatible blob storage is accessible at,
   e.g., `downloads.platform.example.org`. When using the *Download* button
   the platform generates download links under this URL.
4. The URL that the Tenzir Platform CLI connects to, e.g.,
   `api.platform.example.org`.

You must provide the following environment variables to the platform:

```bash
# The domain under which the platform frontend is reachable. Must include the
# `http://` or `https://` scheme.
TENZIR_PLATFORM_DOMAIN=https://app.platform.example.org

# The endpoint to which Tenzir nodes should connect. Must include the `ws://`
# or `wss://` scheme.
TENZIR_PLATFORM_CONTROL_ENDPOINT=wss://nodes.platform.example.org

# The URL at which the platform's S3-compatible blob storage is accessible at.
TENZIR_PLATFORM_BLOBS_ENDPOINT=https://downloads.platform.example.org

# The URL at which the platform's S3-compatible blob storage is accessible at.
TENZIR_PLATFORM_API_ENDPOINT=https://api.platform.example.org
```

### Identity Provider (IdP)

The platform requires an external Identity Provider (IdP) supporting the OIDC
protocol. The IdP must provide valid RS256 ID tokens. The platform must be able
to access the IdP's issuer URL.

You must provide the following environment variables for the OIDC provider
configuration used for logging into the platform:

```bash
TENZIR_PLATFORM_OIDC_PROVIDER_NAME=YOUR_OIDC_PROVIDER_NAME
TENZIR_PLATFORM_OIDC_PROVIDER_CLIENT_ID=YOUR_OIDC_PROVIDER_CLIENT_ID
TENZIR_PLATFORM_OIDC_PROVIDER_CLIENT_SECRET=YOUR_OIDC_PROVIDER_CLIENT_SECRET
TENZIR_PLATFORM_OIDC_PROVIDER_ISSUER_URL=YOUR_OIDC_PROVIDER_ISSUER_URL
```

You must provide the following environment variable containing a JSON object
containing the OIDC issuer and audiences that should be accepted by the
platform.

```bash
TENZIR_PLATFORM_OIDC_TRUSTED_AUDIENCES='{"keycloak.example.org": ["tenzir_platform"]}'
```

You must provide the following environment variable containing a JSON list of
rules granting access to the admin API. The example rule grants admin access to
all users with a valid and signed `id_token` containing the fields
`{"connection": "google-oauth2", "tenzir/org": "TenzirPlatformAdmins"}`.

```bash
TENZIR_PLATFORM_OIDC_ADMIN_RULES='[{"connection": "google-oauth2", "organization_claim": "tenzir/org", "organization": "TenzirPlatformAdmins", "auth_fn": "auth_organization"}]'
```

### PostgreSQL Database

A PostgreSQL database stores the internal state of the platform.

You must provide the following environment variables:

```bash
TENZIR_PLATFORM_POSTGRES_USER=YOUR_POSTGRES_USER
TENZIR_PLATFORM_POSTGRES_PASSWORD=YOUR_POSTGRES_PASSWORD
TENZIR_PLATFORM_POSTGRES_DB=YOUR_POSTGRES_DB
TENZIR_PLATFORM_POSTGRES_HOSTNAME=YOUR_POSTGRES_HOSTNAME
```
