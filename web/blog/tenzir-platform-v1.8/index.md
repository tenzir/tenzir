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

![Tenzir Platform v1.8](tenzir-platform-v1.8.png)

[github-release]: https://github.com/tenzir/platform/releases/tag/v1.8.0

<!-- truncate -->

## Charting

[TODO]

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
