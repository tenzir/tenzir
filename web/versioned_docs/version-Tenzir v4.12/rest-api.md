---
sidebar_position: 0
sidebar_label: REST API
---

# REST API

You can manage a Tenzir through a [REST API](/api). The `web`
[plugin](architecture/plugins.md) implements the server that hosts the API. In
the following, we describe how you can run this web server.

## Deployment Modes

There exist two ways to run the `web` plugin: either as a separate process or
embedded inside a Tenzir node:

![REST API - Single Deployment](rest-api/rest-api-deployment-single.excalidraw.svg)

Running the REST API as dedicated process gives you more flexibility with
respect to deployment, fault isolation, and scaling. An embedded setup offers
higher throughput and lower latency between the REST API and the other Tenzir
components.

To run the REST API as dedicated process, use the `web server` command:

```bash
tenzir-ctl web server --certfile=/path/to/server.certificate --keyfile=/path/to/private.key
```

To run the server within the main Tenzir process, use the `tenzir-node` binary:

```bash
tenzir-node --commands="web server [...]"
```

The server will only accept TLS requests by default. To allow clients to connect
successfully, you need to pass a valid certificate and corresponding private key
with the `--certfile` and `--keyfile` arguments.

## Authentication

Clients must authenticate all requests with a valid token. The token is a short
string that clients put in the `X-Tenzir-Token` request header. You can generate
a valid token on the command line:

```bash
tenzir-ctl web generate-token
```

For local testing and development, generating suitable certificates and tokens
can be a hassle. For this scenario, you can start the server in [developer
mode](#developer-mode) where it accepts plain HTTP connections are does not
perform token authentication.

## TLS Modes

There exist four modes to start the REST API, each of which suits a slightly
different use case.

### Developer Mode

The developer mode bypasses encryption and authentication token verification.

![REST API - Developer Mode](rest-api/rest-api-mode-developer.excalidraw.svg)

Pass `--mode=dev` to start the REST API in developer mode:

```bash
tenzir-ctl web server --mode=dev
```

### Server Mode

The server mode reflects the "traditional" mode of operation where Tenzir binds
to a network interface. This mode only accepts HTTPS connections and requires a
valid authentication token for every request. This is the default mode of
operation.

![REST API - Server Mode](rest-api/rest-api-mode-server.excalidraw.svg)

Pass `--mode=server` to start the REST API in server mode:

```bash
tenzir-ctl web server --mode=server
```

### Upstream TLS Mode

The upstream TLS mode is suitable when Tenzir sits upstream of a separate
TLS terminator that is running on the same machine. This kind of setup
is commonly encountered when running nginx as a reverse proxy.

![REST API - Upstream TLS Mode](rest-api/rest-api-mode-upstream.excalidraw.svg)

Tenzir only listens on localhost addresses, accepts plain HTTP but still
checks authentication tokens.

Pass `--mode=upstream` to start the REST API in server mode:

```bash
tenzir-ctl web server --mode=upstream
```

### Mutual TLS Mode

The mutual TLS mode is suitable when Tenzir sits upstream of a separate TLS
terminator that may be running on a different machine. This setup is commonly
encountered when running nginx as a load balancer. Tenzir would typically be
configured to use a self-signed certificate in this setup.

Tenzir only accepts HTTPS requests, requires TLS client certificates for incoming
connections, and requires valid authentication tokens for any authenticated
endpoints.

![REST API - mTLS Mode](rest-api/rest-api-mode-mtls.excalidraw.svg)

Pass `--mode=mtls` to start the REST API in server mode:

```bash
tenzir-ctl web server --mode=mtls
```

## Scaling

There are two ways to scale the REST API, shall it be the bottleneck. You can
either spawn more REST API actors within Tenzir and expose them at different
ports, or you can spawn more dedicated web server processes:

![REST API - Multi Deployment](rest-api/rest-api-deployment-multi.excalidraw.svg)

We do not anticipate that the web frontend will be on the critical path, since
the web server itself performs very little work. But we get this form of scaling
"for free" as a byproduct of Tenzir's [actor model
architecture](architecture/actor-model.md), which is why we
mentioned it here.
