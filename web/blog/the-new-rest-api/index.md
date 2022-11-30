---
draft: true
title: The New REST API
authors: [lava, mavam]
date: 2022-12-15
image: /img/rest-api-deployment-single.light.png
tags: [frontend, rest, api, architecture]
---

As of [v2.4](vast-v2.4), VAST comes officially with a new `web`
[plugin][plugins] that provides a [REST API][rest-api]. The [API
documentation](/api) describes the available endpoints also provides an
[OpenAPI](https://spec.openapis.org/oas/latest.html) spec for download. This
blog post shows how we built the API and what you can do with it.

[rest-api]: /docs/use/integrate/rest-api
[plugins]: /docs/understand/architecture/plugins
[actors]: /docs/understand/architecture/actor-model

<!--truncate-->

Two architectural features of VAST made it really easy to design the REST API:

1. [Plugins][plugins]
2. [Actors][actors]

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

![REST API - Single Deployment](/img/rest-api-deployment-single.light.png#gh-light-mode-only)
![REST API - Single Deployment](/img/rest-api-deployment-single.dark.png#gh-dark-mode-only)

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
mode](#developer-mode) where it accepts plain HTTP connections are does not
perform token authentication.

## TLS Modes

There exist four modes to start the REST API, each of which suits a slightly
different use case.

### Developer Mode

The developer mode bypasses encryption and authentication token verification.

![REST API - Developer Mode](/img/rest-api-developer-mode.light.png#gh-light-mode-only)
![REST API - Developer Mode](/img/rest-api-developer-mode.dark.png#gh-dark-mode-only)

Pass `--mode=dev` to start the REST API in developer mode:

```bash
vast web server --mode=dev
```

### Server Mode

The server mode reflects the "traditional" mode of operation where VAST binds to
a network interface. This mode only accepts HTTPS connections and requires a
valid authentication token for every request. This is the default mode of
operation.

![REST API - Server Mode](/img/rest-api-server-mode.light.png#gh-light-mode-only)
![REST API - Server Mode](/img/rest-api-server-mode.dark.png#gh-dark-mode-only)

Pass `--mode=server` to start the REST API in server mode:

```bash
vast web server --mode=server
```

### Upstream TLS Mode

The upstream TLS mode is suitable when VAST sits upstream of a separate
TLS terminator that is running on the same machine. This kind of setup
is commonly encountered when running nginx as a reverse proxy.

![REST API - TLS Upstream Mode](/img/rest-api-tls-upstream-mode.light.png#gh-light-mode-only)
![REST API - TLS Upstream Mode](/img/rest-api-tls-upstream-mode.dark.png#gh-dark-mode-only)

VAST only listens on localhost addresses, accepts plain HTTP but still
checks authentication tokens.

Pass `--mode=upstream` to start the REST API in server mode:

```bash
vast web server --mode=upstream
```

### Mutual TLS Mode

The mutual TLS mode is suitable when VAST sits upstream of a separate TLS
terminator that may be running on a different machine. This setup is commonly
encountered when running nginx as a load balancer. VAST would typically be
configured to use a self-signed certificate in this setup.

VAST only accepts HTTPS requests, requires TLS client certificates for incoming
connections, and requires valid authentication tokens for any authenticated
endpoints.

![REST API - Mutual TLS Mode](/img/rest-api-mutual-tls-mode.light.png#gh-light-mode-only)
![REST API - Mutual TLS Mode](/img/rest-api-mutual-tls-mode.dark.png#gh-dark-mode-only)

Pass `--mode=mtls` to start the REST API in server mode:

```bash
vast web server --mode=mtls
```

## Usage Examples

Now that you know how we put the REST API together, let's look at some
end-to-end examples. We built the REST API for two reasons::

1. Enable third parties to integrate with VAST
2. Develop our own web frontend

We'll conclude this blog post with few examples that show how you can use the
REST API.

:::caution TODO
Showcase `/status`, `/export` and `/query` route.
:::

As always, if you have any question on usage, swing by our [community
Slack](http://slack.tenzir.com). Missing routes? Let us know so that we know
what to prioritize. Now happy curling! :curling_stone:
