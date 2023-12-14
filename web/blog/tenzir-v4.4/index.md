---
title: Tenzir v4.4
authors: [dominiklohmann]
date: 2023-11-06
last_updated: 2023-12-12
tags: [release, operators, velociraptor, yara, amqp]
comments: true
---

[Tenzir v4.4](https://github.com/tenzir/tenzir/releases/tag/v4.4.0) is out!
We've focused this release on integrations with two pillars of the digital
forensics and incident response (DFIR) ecosystem: [YARA][yara] and
[Velociraptor][velociraptor].

[yara]: https://yara.readthedocs.io
[velociraptor]: https://docs.velociraptor.app

![Tenzir v4.4](tenzir-v4.4.excalidraw.svg)

<!-- truncate -->

## YARA Operator

The star feature of this release is the new [`yara`](/next/operators/yara)
operator. You can now match [YARA][yara] rules directly within byte pipelines.
This is a game-changer for threat intelligence and cybersecurity workflows, as
it brings together all of Tenzir's connectors with the community's rich
ecosystem of YARA rules for efficient malware detection and analysis. Evaluating
a set of rules on a file located in an S3 bucket has never been easier:

```
load s3 bucket/file.exe
| yara path/to/rules/
```

:::info
We've written a blog post on the YARA operator that shows just how it works and
explains in-depth how you can use it: [Matching YARA Rules in Byte
Pipelines](/blog/matching-yara-rules-in-byte-pipelines)
:::

## Velociraptor Operator

[Velociraptor][velociraptor] is an advanced DFIR tool that enhances your
visibility into your endpoints. Not unlike [our own TQL](/language),
Velociraptor comes with its own language for interacting with it
programmatically: VQL. The `velociraptor` operator makes it possible to submit
VQL queries to a Velociraptor server, as well as subscribe to artifacts
in hunt flows over a large fleet of assets, making endpoint telemetry
collection and processing a breeze.

:::info
Read our blog post on how we built this integration and how you can utilize it:
[Integrating Velociraptor into Tenzir
Pipelines](/blog/integrating-velociraptor-into-tenzir-pipelines)
:::

## AMQP Connector

The new [`amqp`](/next/connectors/amqp) connector brings a full-fledged AMQP
0-9-1 client to the table. Relying on the battle-proven [RabbitMQ C client
library](https://github.com/alanxz/rabbitmq-c), the operator makes it possible
you to interact with queues and exchanges as shown in the diagram below:

![AMQP](amqp.excalidraw.svg)

## Noteworthy Improvements

Besides the new operators, I would like to highlight the following changes:

- **Live Exports:** Start your pipeline with `export --live` to get all events
  in one pipeline as they are imported.

- **Blob Type:** We've added a new `blob` type that allows you to handle binary
  data. Use the `blob` type over the `string` type for binary payloads that are
  not UTF8-encoded.

- **Rich Schema Inference for CSV:** Inferring schemas for CSV files has been
  significantly enhanced. It now provides more precise types, leading to more
  insightful analysis.

- **Automated Pipeline Management:** New controls for auto-restart, auto-delete
  and a runtime limit are now available when creating a pipeline. For a more
  granular control of the auto-restart and auto-delete behavior, the _Stopped_
  state for pipeline has now been divided into _Stopped_, _Completed_, and
  _Failed_. The states reflect whether a pipeline was manually stopped, ended
  naturally, or encountered an error, respectively.

- **Label Support for Pipelines:** You can now visually group related pipelines
  using the new labels feature. This helps you in organizing your pipelines
  better for improved visibility and accessibility.

We provide a full list of changes [in our changelog](/changelog#v440).

Check out the new features on [app.tenzir.com](https://app.tenzir.com). We're
excited to see the amazing things you will accomplish with them!

Your feedback matters and drives our growth. Join the discussion in [our
Discord](/discord)!
