---
title: The New REST API
authors: [lava, mavam]
date: 2023-01-26
last_updated: 2023-02-08
image: /img/blog/rest-api-deployment-single.excalidraw.svg
tags: [frontend, rest, api, architecture]
---

As of [v2.4](/blog/vast-v2.4) VAST ships with a new `web` [plugin][plugins] that
provides a [REST API][rest-api]. The [API documentation](/api) describes the
available endpoints also provides an
[OpenAPI](https://spec.openapis.org/oas/latest.html) spec for download. This
blog post shows how we built the API and what you can do with it.

[rest-api]: /docs/VAST%20v3.0/use/integrate/rest-api
[plugins]: /docs/VAST%20v3.0/understand/architecture/plugins
[actors]: /docs/VAST%20v3.0/understand/architecture/actor-model

<!--truncate-->

Why does VAST need a REST API? Two reasons:

1. **Make it easy to integrate with VAST**. To date, the only interface to VAST
   is the command line. This is great for testing and ad-hoc use cases, but to
   make it easy for other tools to integrate with VAST, a REST API is the common
   expectation.

2. **Develop our own web frontend**. We are in the middle of building a
   [Svelte](https://svelte.dev/) frontend that delivers a web-based experience
   of interacting with VAST through the browser. This frontend interacts with
   VAST through the REST API.

Two architectural features of VAST made it really smooth to design the REST API:
[Plugins][plugins] and [Actors][actors].

First, VAST's plugin system offers a flexible extension mechanism to add
additional functionality without bloating the core. Specifically, we chose
[RESTinio](https://github.com/Stiffstream/restinio) as C++ library that
implements an asynchronous HTTP and WebSocket server. Along with it comes a
dependency on Boost ASIO. We deem it acceptable to have this dependency of the
`web` plugin, but would feel less comfortable with adding dependencies to the
VAST core, which we try to keep as lean as possible.

Second, the [actor model architecture][actors] of VAST makes it easy to
integrate new "microservices" into the system. The `web` plugin is a *component
plugin* that provides a new actor with a typed messaging interface. It neatly
fits into the existing architecture and thereby inherits the flexible
distribution and scaling properties. Concretely, there exist two ways to run the
REST API actor: either as a separate process or embedded inside a VAST server
node:

![REST API - Single Deployment](rest-api-deployment-single.excalidraw.svg)

Running the REST API as dedicated process gives you more flexibility with
respect to deployment, fault isolation, and scaling. An embedded setup offers
higher throughput and lower latency between the REST API and the other VAST
components.

The REST API is also a *command plugin* and exposes the—you guessed it—`web`
command. To run the REST API as dedicated process, spin up a VAST node as
follows:

```bash
vast web server --certfile=/path/to/server.certificate --keyfile=/path/to/private.key
```

To run the server within the main VAST process, use a `start` command:

```bash
vast start --commands="web server [...]"
```

The server will only accept TLS requests by default. To allow clients to connect
successfully, you need to pass a valid certificate and corresponding private key
with the `--certfile` and `--keyfile` arguments.

## Authentication

Clients must authenticate all requests with a valid token. The token is a short
string that clients put in the `X-VAST-Token` request header.

You can generate a valid token on the command line as follows:

```bash
vast web generate-token
```

For local testing and development, generating suitable certificates and tokens
can be a hassle. For this scenario, you can start the server in [developer
mode](#developer-mode) where it accepts plain HTTP connections and does not
perform token authentication.

## TLS Modes

There exist four modes to start the REST API, each of which suits a slightly
different use case.

### Developer Mode

The developer mode bypasses encryption and authentication token verification.

![REST API - Developer Mode](rest-api-mode-developer.excalidraw.svg)

Pass `--mode=dev` to start the REST API in developer mode:

```bash
vast web server --mode=dev
```

### Server Mode

The server mode reflects the "traditional" mode of operation where VAST binds to
a network interface. This mode only accepts HTTPS connections and requires a
valid authentication token for every request. This is the default mode of
operation.

![REST API - Server Mode](rest-api-mode-server.excalidraw.svg)

Pass `--mode=server` to start the REST API in server mode:

```bash
vast web server --mode=server
```

### Upstream TLS Mode

The upstream TLS mode is suitable when VAST sits upstream of a separate
TLS terminator that is running on the same machine. This kind of setup
is commonly encountered when running nginx as a reverse proxy.

![REST API - Developer Mode](rest-api-mode-developer.excalidraw.svg)

VAST only listens on localhost addresses, accepts plain HTTP but still
checks authentication tokens.

Pass `--mode=upstream` to start the REST API in server mode:

```bash
vast web server --mode=upstream
```

### Mutual TLS Mode

The mutual TLS mode is suitable when VAST sits upstream of a separate TLS
terminator that may be running on a different machine. In this scenario,
the connection between the terminator and VAST must again be encrypted
to avoid leaking the authentication token to the network.

Regular TLS requires only the server to present a certificate to prove his
identity. In mutual TLS mode, the client additionally needs to provide a
valid *client certificate* to the server. This ensures that the TLS terminator
cannot be impersonated or bypassed.

Typically self-signed certificates are used for that purpose, since both ends of
the connection are configured together and not exposed to the public internet.

![REST API - mTLS Mode](rest-api-mode-mtls.excalidraw.svg)

Pass `--mode=mtls` to start the REST API in mutual TLS mode:

```bash
vast web server --mode=mtls
```

## Usage Examples

Now that you know how we put the REST API together, let's look at some
end-to-end examples.

### See what's inside VAST

One straightforward example is checking the number of records in VAST:

```bash
curl "https://vast.example.org:42001/api/v0/status?verbosity=detailed" \
  | jq .index.statistics
```

```json
{
  "events": {
    "total": 8462
  },
  "layouts": {
    "zeek.conn": {
      "count": 8462,
      "percentage": 100
    }
  }
}
```

:::caution Status changes in v3.0
In the upcoming v3.0 release, the statistics under the key `.index.statistics`
will move to `.catalog`. This change is already merged into the master branch.
Consult the [status key reference](/docs/VAST%20v3.0/setup/monitor#reference) for details.
:::

### Perform a HTTP health check

The `/status` endpoint can also be used as a HTTP health check in
`docker-compose`:

```yaml
version: '3.4'
services:
  web:
    image: tenzir/vast
    environment:
      - "VAST_START__COMMANDS=web server --mode=dev"
    ports:
      - "42001:42001"
    healthcheck:
      test: curl --fail http://localhost:42001/status || exit 1
      interval: 60s
      retries: 5
      start_period: 20s
      timeout: 10s
```

### Run a query

The other initial endpoints can be used to get data out of VAST. For example, to
get up to two `zeek.conn` events which connect to the subnet `192.168.0.0/16`, using
the VAST query expression `net.src.ip in 192.168.0.0/16`:

```bash
curl "http://127.0.0.1:42001/api/v0/export?limit=2&expression=net.src.ip%20in%20192.168.0.0%2f16"
```

```json
{
  "version": "v2.4.0-457-gb35c25d88a",
  "num_events": 2,
  "events": [
    {
      "ts": "2009-11-18T08:00:21.486539",
      "uid": "Pii6cUUq1v4",
      "id.orig_h": "192.168.1.102",
      "id.orig_p": 68,
      "id.resp_h": "192.168.1.1",
      "id.resp_p": 67,
      "proto": "udp",
      "service": null,
      "duration": "163.82ms",
      "orig_bytes": 301,
      "resp_bytes": 300,
      "conn_state": "SF",
      "local_orig": null,
      "missed_bytes": 0,
      "history": "Dd",
      "orig_pkts": 1,
      "orig_ip_bytes": 329,
      "resp_pkts": 1,
      "resp_ip_bytes": 328,
      "tunnel_parents": []
    },
    {
      "ts": "2009-11-18T08:08:00.237253",
      "uid": "nkCxlvNN8pi",
      "id.orig_h": "192.168.1.103",
      "id.orig_p": 137,
      "id.resp_h": "192.168.1.255",
      "id.resp_p": 137,
      "proto": "udp",
      "service": "dns",
      "duration": "3.78s",
      "orig_bytes": 350,
      "resp_bytes": 0,
      "conn_state": "S0",
      "local_orig": null,
      "missed_bytes": 0,
      "history": "D",
      "orig_pkts": 7,
      "orig_ip_bytes": 546,
      "resp_pkts": 0,
      "resp_ip_bytes": 0,
      "tunnel_parents": []
    }
  ]
}
```

Note that when using `curl`, all request parameters need to be properly
urlencoded. This can be cumbersome for the `expression` and `pipeline`
parameters, so we also provide an `/export` POST endpoint that accepts
parameters in the JSON body. The next example shows how to use POST requests
from curl. It also uses the `/query` endpoint instead of `/export` to get
results iteratively instead of a one-shot result. The cost for this is having to
make two API calls instead of one:

```bash
curl -XPOST -H"Content-Type: application/json" -d'{"expression": "udp"}' http://127.0.0.1:42001/api/v0/query/new
```

```json
{"id": "31cd0f6c-915f-448e-b64a-b5ab7aae2474"}
```

```bash
curl http://127.0.0.1:42001/api/v0/query/31cd0f6c-915f-448e-b64a-b5ab7aae2474/next?n=2 | jq
```

```json
{
  "position": 0,
  "events": [
    {
      "ts": "2009-11-18T08:00:21.486539",
      "uid": "Pii6cUUq1v4",
      "id.orig_h": "192.168.1.102",
      "id.orig_p": 68,
      "id.resp_h": "192.168.1.1",
      "id.resp_p": 67,
      "proto": "udp",
      "service": null,
      "duration": "163.82ms",
      "orig_bytes": 301,
      "resp_bytes": 300,
      "conn_state": "SF",
      "local_orig": null,
      "missed_bytes": 0,
      "history": "Dd",
      "orig_pkts": 1,
      "orig_ip_bytes": 329,
      "resp_pkts": 1,
      "resp_ip_bytes": 328,
      "tunnel_parents": []
    },
    {
      "ts": "2009-11-18T08:08:00.237253",
      "uid": "nkCxlvNN8pi",
      "id.orig_h": "192.168.1.103",
      "id.orig_p": 137,
      "id.resp_h": "192.168.1.255",
      "id.resp_p": 137,
      "proto": "udp",
      "service": "dns",
      "duration": "3.78s",
      "orig_bytes": 350,
      "resp_bytes": 0,
      "conn_state": "S0",
      "local_orig": null,
      "missed_bytes": 0,
      "history": "D",
      "orig_pkts": 7,
      "orig_ip_bytes": 546,
      "resp_pkts": 0,
      "resp_ip_bytes": 0,
      "tunnel_parents": []
    }
  ]
}
```

:::note Still Experimental
Please note that we consider the API version `v0` experimental, and we make no
stability guarantees at the moment.
:::

As always, if you have any question on usage, swing by our [community
chat](/discord). Missing routes? Let us know so that we know
what to prioritize. Now happy curling! :curling_stone:
