---
title: "Tenzir Platform v1.8: Charting the Unknown"
slug: tenzir-platform-v1.8
authors: [lava, raxyte]
date: 2025-01-08
tags: [release, platform]
comments: true
---

We're happy to announce [Tenzir Platform v1.8][github-release], with
new and improved charting as well as a new single-user mode for
Sovereign Edition users.

<!-- ![Tenzir Platform v1.8](tenzir-platform-v1.8.png) -->

[github-release]: https://github.com/tenzir/platform/releases/tag/v1.8.0

<!-- truncate -->

## Charting

[Tenzir Node v4.27.0](/blog/tenzir-node-v4.27) introduced four new charting operators for
TQL2 -- `chart_line`, `chart_area`, `chart_bar` and `chart_pie`.

These new operators offer easy ways to aggregate, pivot, bucket and visualize
your data. Combine these with [metrics](/tql2/operators/metrics) from the Tenzir Node and you have a
powerful way to digest information about Node health and more.

### Examples

#### Visualizing load

TODO: Explain pipeline

```tql
metrics "cpu"
chart_area x=timestamp, y={"Avg. Load": mean(loadavg_1m), "Max. Load": max(loadavg_1m)},
          resolution=30min,
          x_min=now()-5d,
          y_max=10
```

![Node Load](node-load.png)

#### Visualizing schema counts

TODO: Explain pipeline

```tql
metrics "import"
chart_pie label=schema, value=sum(events)
```

![Schema counts](schema-pie.png)

:::tip Dashboards
Charts can be added to your [Tenzir
dashboards](https://app.tenzir.com/dashboards) to be readly viewed at any time.

TODO: How?

![Add to dashboard](add-to-dashboard.png)
:::

## Single-User Mode

We added a new, simplified example setup for users of the Sovereign Edition
that gets rid of all manual setup and just has a single default user that
is automatically signed in.

To try it out, just start a docker compose stack in the local platform
checkout.

```sh
git clone https://github.com/tenzir/platform
cd platform/examples/localdev
docker compose up
```

## Native TLS

The Tenzir Platform extended its support for native TLS support in
all containers. This is relevant where encrypted connections between
the reverse proxy and the backend containers are desired.

The flags are:

```
platform / websocket-gateway:
TLS_CERTFILE=
TLS_KEYFILE=
TLS_CAFILE=

app:
TLS_CERTFILE=
TLS_KEYFILE=

```

The logic for all of these is that if CERTFILE and KEYFILE are present,


## Bug fixes

This release also contains several additional bugfixes:

- [...]

## Join Us for Office Hours

Join us for our bi-weekly office hours every other Tuesday at 5 PM CET on our
[Discord server][discord]. It's a great opportunity to share your experiences,
ask questions, and help shape the future of Tenzir with your valuable feedback!

[discord]: /discord
