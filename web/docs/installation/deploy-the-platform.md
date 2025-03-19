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
TENZIR_PLATFORM_CLI_API_ENDPOINT=api.platform.example:5000
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
   `app.platform.example`. This serves a web frontend where the user can
   interact with the platform.
2. The URL that the nodes connect to, e.g., `nodes.platform.example`. Tenzir
   Nodes connect to this URL to establish long-running WebSocket connections.
3. The URL that the platform's S3-compatible blob storage is accessible at,
   e.g., `downloads.platform.example`. When using the *Download* button
   the platform generates download links under this URL.
4. The URL that the Tenzir Platform CLI connects to, e.g.,
   `api.platform.example`.

You must provide the following environment variables to the platform:

```bash
# The domain under which the platform frontend is reachable. Must include the
# `http://` or `https://` scheme.
TENZIR_PLATFORM_DOMAIN=https://app.platform.example

# The endpoint to which Tenzir nodes should connect. Must include the `ws://`
# or `wss://` scheme.
TENZIR_PLATFORM_CONTROL_ENDPOINT=wss://nodes.platform.example

# The URL at which the platform's S3-compatible blob storage is accessible at.
TENZIR_PLATFORM_BLOBS_ENDPOINT=https://downloads.platform.example

# The URL at which the platform's S3-compatible blob storage is accessible at.
TENZIR_PLATFORM_API_ENDPOINT=https://api.platform.example
```

### Identity Provider (IdP)

The Tenzir Platform can be configured to use an external Identity Provider.
This is required when using the platform with multiple users.

#### IdP Requirements

In order to use an external identity provider, it must support the OIDC protocol
including the OIDC Discovery extension, and it must be configured to provide valid
RS256 ID tokens.

For full features of the platform, two clients (also called Applications in Auth0 or
App Registrations in Microsoft Entra) need to be created which we will call `tenzir-app`
and `tenzir-cli` below.

The `tenzir-app` client is used for logging into the Tenzir Platform in the web
browser.

- The **Authorization Code** flow must be enabled.
- The allowed redirect URLs must include `https://app.platform.example/login/oauth/callback`.
- The client secret should be noted down so it can be added to the configuration
   of the Tenzir Platform in the next step.

The `tenzir-cli` client is used to authenticate with the `tenzir-platform` CLI.

- The **Device Code** flow must be enabled.
- If the identity provider does not return an `id_token` for the device code
  flow, then the returned `access_token` must be in JWT format.

Often it is desired to run CLI commands in environments where no user
is available to perform the device code authorization flow, for example
when running CLI commands as part of a CI job.

In this case, a third client can be set up with the **Client Credentials** flow
enabled. The `access_token` obtained from this client must be in JWT format.
The CLI can make use of the client credentials flow by using
the `tenzir-platform auth login --non-interactive` option.

#### Platform Configuration

You must provide the following environment variables for the OIDC provider
configuration used for logging into the platform:

```bash
TENZIR_PLATFORM_OIDC_PROVIDER_NAME=example-idp
TENZIR_PLATFORM_OIDC_PROVIDER_ISSUER_URL=https://my.idp.example

TENZIR_PLATFORM_OIDC_CLI_CLIENT_ID=tenzir-cli

TENZIR_PLATFORM_OIDC_APP_CLIENT_ID=tenzir-app
TENZIR_PLATFORM_OIDC_APP_CLIENT_SECRET=xxxxxxxxxxxxxxxxxxxxxxxx
```

You must provide the following environment variable containing a JSON object
containing the OIDC issuer and audiences that should be accepted by the
platform.

```bash
TENZIR_PLATFORM_OIDC_TRUSTED_AUDIENCES='{"https://my.idp.example": ["tenzir-cli", "tenzir-app"]}'
```

You must configure the set of `admin` users in your platform instance. An admin
user is a user who is permitted to run the `tenzir-platform admin` cli command.
This is done by configuring a list of rules that are evaluated against the ID tokens
provided by the OIDC  the following environment variable containing a JSON list of
rules granting access to the admin API. If any of the provided rules match, the user
is considered to be an admin. The example rule grants admin access to all users with
a valid and signed `id_token` containing the fields `{"tenzir/org": "TenzirPlatformAdmins"}`.

```bash
TENZIR_PLATFORM_OIDC_ADMIN_RULES='[{"organization_claim": "tenzir/org", "organization": "TenzirPlatformAdmins", "auth_fn": "auth_organization"}]'
```

See the documentation on [Access Rules](/platform-cli#configure-access-rules) for
more information about the possible types of rules and their syntax. The `-d` option of
the CLI can be used to generate valid JSON objects that can be entered here.

### PostgreSQL Database

A PostgreSQL database stores the internal state of the platform.

You must provide the following environment variables:

```bash
TENZIR_PLATFORM_POSTGRES_USER=YOUR_POSTGRES_USER
TENZIR_PLATFORM_POSTGRES_PASSWORD=YOUR_POSTGRES_PASSWORD
TENZIR_PLATFORM_POSTGRES_DB=YOUR_POSTGRES_DB
TENZIR_PLATFORM_POSTGRES_HOSTNAME=YOUR_POSTGRES_HOSTNAME
```

### TLS Settings

We strongly recommend using signed TLS certificates which are trusted by the machines
running the Tenzir Nodes.

However, it can sometimes be necessary to use self-signed certificates for the Tenzir Platform.
 in this situation we recommend the creation of a local certificate authority, e.g., using the
[trustme](https://pypi.org/project/trustme/) project. This approach has the advantage of not
requiring the client to disable TLS certificate validation, thus preventing man-in-the-middle
attacks when compared to a self-signed certificate..

If that is not possible, a self-signed certificate can be generated using `openssl`
by following [this procedure](https://stackoverflow.com/a/10176685/92560).

#### Node Settings

On the node, in order to trust a custom CA certificate, the following option
needs to point to a CA certificate file in PEM format without password protection:

```env
TENZIR_PLATFORM_CACERT=/path/to/ca-certificate.pem
```

If you want to use a self-signed TLS certificate, you need to disable
TLS certificate validation by setting:

```env
TENZIR_PLATFORM_SKIP_PEER_VERIFICATION=true
```

Note that these settings only apply to the connection that is established
between the node and the platform, not to any TLS connections that may
be created by running pipelines on the node.
