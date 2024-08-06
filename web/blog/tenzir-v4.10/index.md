---
title: Tenzir v4.10
authors: [dominiklohmann]
date: 2024-03-11
tags: [release, pipelines-as-code, contexts, arm64]
comments: true
---

Today, we're releasing [Tenzir
v4.10](https://github.com/tenzir/tenzir/releases/tag/v4.10.0), which improves
how Tenzir integrates with modern deployment practices.

![Tenzir v4.10](tenzir-v4.10.excalidraw.svg)

<!-- truncate -->

## Pipelines as Code

In today's deployment landscape, best practices emphasize GitOps in synergy with
Infrastructure as Code (IaC). With the goal of integrating our services into
these existing mechanisms, we're excited to introduce Pipelines as Code (PaC) in
Tenzir v4.10.

PaC differs from traditional deployment methods in two key aspects. Firstly,
pipelines deployed as code always start with the Tenzir node, ensuring
continuous operation. Secondly, to safeguard them, deletion via the user
interface is disallowed for pipelines deployed as code.

Here's a simple example to get you started:

```yaml {0} title="<prefix>/etc/tenzir/tenzir.yaml"
tenzir:
  pipelines:
    suricata-over-tcp:
      name: Import Suricata from TCP
      definition: |
        from tcp://0.0.0.0:34343 read suricata
        | import
      start:
        failed: true  # always restart on failure
```

:::tip Want to learn more?
Read our guide on PaC: 👉 [Deploy Pipelines as
Code](/usage/run-pipelines#as-code)
:::

## arm64 Docker Images

Did you ever try to run Tenzir in Docker on a new-ish MacBook and encountered
this error?

```text {0} title="❯ docker run tenzir/tenzir:v4.9.0 version"
WARNING: The requested image's platform (linux/amd64) does not match the detected host platform (linux/arm64/v8) and no specific platform was requested
tenzir: error while loading shared libraries: libfluent-bit.so: cannot enable executable stack as shared object requires: Invalid argument
```

Now, this works as expected:

```json {0} title="❯ docker run tenzir/tenzir:v4.10.0 version"
{
  "version": "4.10.0",
  "build": "",
  "major": 4,
  "minor": 10,
  "patch": 0
}
```

This works because the Tenzir Docker images now are multi-archecture images
built natively for both `linux/amd64` and `linux/arm64/v8`. In addition to
supporting M-series MacBooks, this also allows the Docker images to run without
emulation on other arm64-based systems like AWS Graviton.

## Reimagining Unsafe Pipelines

We've substituted the `tenzir.allow-unsafe-pipelines` feature with
`tenzir.no-location-overrides`, flipping the default set-up and enhancing user
experience.

`tenzir.allow-unsafe-pipelines` had been historically puzzling for newcomers
given its seemingly fearsome name and ambiguous implications. Why would someone
consciously permit unsafe pipelines? And why have we now defaulted to allowing
them?

Pipelines have the ability to execute in multiple processes. For instance,
executing `tenzir 'from file.json | import'` would prompt `from file.json` to
run in the `tenzir` process, and `import` in the connected `tenzir-node`
process. An operator's _location_ can be assigned as local, anywhere, or remote.
On initializing a pipeline, Tenzir's executor intelligently divides the pipeline
according to location change between local and remote, starts separated
pipelines at their respective locations, connects them to one another.

However, operator locations can also be manually manipulated. For instance, when
capturing PCAPs, users might desire to prevent unnecessary inter-process
communication and directly connect the Tenzir Node to the network
interface—achieved by executing `tenzir 'remote from nic …'`. This command
instructs the executor to consistently run `from nic …` directly at the node.
When introducing this feature during the Tenzir v4.0 release, we wanted to be
cautious about unrestricted use of this feature, leading to the creation of the
`tenzir.allow-unsafe-pipelines` option, which by default was set to false. This
option prohibits the use of location overrides when enabled but simultaneously
posed puzzlement to new users being the lone feature disallowed in an "unsafe"
pipeline.

In response to feedback, we've improved our approach. Location overrides are now
permitted by default and can be disallowed by using the new option
`tenzir.no-location-overrides`.

## Apply Contexts to Multiple Fields

Did you ever want to act on multiple fields in `enrich` or `lookup`? Now you
can!

For example, you can now use a [GeoIP context](/contexts/geoip) on all IP
addresses in your data as simple as this:

```text {0} title="Enrich with a geoip context named country"
…
| enrich country --field :ip
```

You can also specify multiple fields explicitly:

```
…
| enrich country --field src_ip,dest_ip
```

The output of `lookup` and `enrich` changed slightly to accomodate multiple
contexts in the same event. Under the output field (that defaults to the context
name), there is now a new record named `context`, under which we replicate the
path to the enriched fields for placing the context. That is, the context of
`id.orig_h` in this example is accessible as `country.context.id.orig_h`:

```json {0} title="export | enrich country"
{
  "ts": "2021-11-17T13:53:51.022351",
  "uid": "CVtvt83MWz8MBNTWWd",
  "id": {
    "orig_h": "244.69.36.0",
    "orig_p": 45228,
    "resp_h": "242.239.167.49",
    "resp_p": 34774
  },
  "proto": "udp",
  // ...
  "country": {
    "timestamp": "2024-03-11T15:58:00.596027",
    "mode": "enrich",
    "context": {
      "id": {
        "orig_h": {
          "country": {
            "geoname_id": 1861060,
            "iso_code": "JP",
            "names": {
              "de": "Japan",
              "en": "Japan",
              "es": "Japón",
              "fr": "Japon",
              "ja": "日本",
              "pt-BR": "Japão",
              "ru": "Япония",
              "zh-CN": "日本"
            }
          }
        },
        "resp_h": {
          "country": {
            "geoname_id": 1861060,
            "iso_code": "JP",
            "names": {
              "de": "Japan",
              "en": "Japan",
              "es": "Japón",
              "fr": "Japon",
              "ja": "日本",
              "pt-BR": "Japão",
              "ru": "Япония",
              "zh-CN": "日本"
            }
          }
        }
      }
    }
  }
}
```

## Other Changes

For the curious, [the changelog](/changelog#v4100) includes the full list of bug
fixes, changes and improvements introduced with this release.

Play with the new features at [app.tenzir.com](https://app.tenzir.com) and join
us on [our Discord server](/discord).
