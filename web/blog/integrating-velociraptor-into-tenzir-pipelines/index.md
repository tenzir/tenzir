---
title: Integrating Velociraptor into Tenzir Pipelines
authors: mavam
date: 2023-10-17
tags: [velociraptor, operator, dfir]
draft: true
---

The new [`velociraptor`][velociraptor-operator] operator allows you to run
[Velociraptor Query Language (VQL)][vql] expressions against a
[Velociraptor][velociraptor] server and process the results in a Tenzir
pipeline. You can also subscribe to matching artifacts in hunt flows over a
large fleet of assets, making endpoint telemetry collection and processing a
breeze.

[velociraptor]: https://docs.velociraptor.app/
[velociraptor-operator]: /next/operators/sources/velociraptor
[vql]: https://docs.velociraptor.app/docs/vql

![Velociraptor and Tenzir](velociraptor-and-tenzir.excalidraw.svg)

<!--truncate-->

[Velociraptor][velociraptor] is a powerful digital forensics and incident
response (DFIR) tool for managing and interrogating endpoints. Not only does it
support ad-hoc extraction of forensic artifacts, but also continuous event
monitoring to get alerted when suspicious things happen, such as the
installation of new scheduled tasks on a Windows machine.

We have been asked to make it possible to process the data collected at
endpoints in a Tenzir pipeline, so that you can store it cost-effectively,
filter it, reshape it, and route it to your destination of choice. The
`velociraptor` operator honors this request. Thanks to Velociraptor's [gRPC
API][api] and [Python library][pyvelociraptor] that ship with the
[Protobuf][proto] definition, the implementation in C++ was straight-forward.

[api]: https://docs.velociraptor.app/docs/server_automation/server_api/
[pyvelociraptor]: https://github.com/Velocidex/pyvelociraptor
[proto]: https://github.com/Velocidex/pyvelociraptor/blob/master/pyvelociraptor/api.proto

## Usage

The `velociraptor` is a source that emits events. We implemented two ways to
interact with a Velociraptor server:

1. Send a [VQL][vql] query to a server and process the response.

2. Use the `--subscribe <artifact>` option to hook into a continuous feed of
   artifacts that match the `<artifact>` regular expression. Whenever a hunt
   runs that contains this artifact, the server will forward it to the pipeline
   and emit the artifact payload in the response field `HuntResults`.

### Raw VQL

Here's how you execute a VQL query and store the result at a Tenzir node:

```bash
velociraptor --query "select * from pslist()"
| import
```

Storing it via [`import`](/operators/sinks/import) is just one of many options.
For ad-hoc investigations, you often just want to analyze the result, for which
a variety of [transformations](/operators/transformations) come in handy. For
example:

```bash
velociraptor --query "select * from pslist()"
| select Name, Pid, PPid, CommandLine
| where Name == "remotemanagement"
```

### Artifact Subscription

TODO: provide examples for the subscription use case, e.g.,

```bash
velociraptor --subscribe Windows
```

## Preparation

The `velociraptor` pipeline operator acts as client and it establishes a
connection to a Velociraptor server via gRPC. All Velociraptor client-to-server
communication is mutually authenticated and encrypted via TLS certificates. This
means you must provide a client-side certificate, which you can generate as
follows. (Velociraptor ships as a static binary that we
refer to as `velociraptor-binary` here.)

1. Create a server configuration `server.yaml`:
   ```bash
   velociraptor-binary config generate > server.yaml
   ```

1. Create a `tenzir` user with `api` role:
   ```bash
   velociraptor-binary -c server.yaml user add --role=api tenzir
   ```

1. Run the frontend with the server configuration:
   ```bash
   velociraptor-binary -c server.yaml frontend
   ```

1. Create an API client:
   ```bash
   velociraptor-binary -c server.yaml config api_client --name tenzir client.yaml
   ```

1. Copy the generated `client.yaml` to your Tenzir plugin configuration
   directory as `velociraptor.yaml` so that the operator can find it:
   ```bash
   cp client.yaml /etc/tenzir/plugin/velociraptor.yaml
   ```

Now you are ready to run VQL queries!

:::note Acknowledgements
Big thanks for to VQL wizard [Christoph Lobmeyer](https://github.com/lo-chr) who
contributed the intricate expression that is behind the `--subscribe <artifact>`
option. 🙏
:::
