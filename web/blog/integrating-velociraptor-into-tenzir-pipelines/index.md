---
title: Integrating Velociraptor into Tenzir Pipelines
authors:
   - mavam
   - name: Christoph Lobmeyer
    title: Senior Expert Incident Response (External)
    url: https://github.com/lo-chr
    image_url: https://github.com/lo-chr.png
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
   artifacts that match the `<artifact>` regular expression. Whenever a client responds to a hunt
   that contains this artifact, the response will be forwarded to the pipeline
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

If you use Velociraptor to perform interactive investigations in DFIR cases, you probably hunt for forensic artifacts (like dropped files or specific entries in the Windows registry) on assets connected to your Velociraptor server. For enrichment or to correlate the results with other security related data, you might want to post-process results of Velociraptor hunts.

With this feature Tenzir can subscribe to results of hunts, containing Velociraptor artifacts of your choice [like the ones shipped with Velociraptor](https://docs.velociraptor.app/artifact_references/). Everytime a client reports back on an artifact that matches the given Regex (like `Windows` or `Windows.Sys.StartupItems`) Tenzir will ingest the result of the underlying query into the pipeline.

```bash
velociraptor --subscribe Windows.Sys.StartupItems | import
```

There are many examples of anomalies to search for, like malware families persisting in Windows RunKeys. You can find some inspirations in the procedure examples of [MITRE ATT&CK Sub-Technique T1547.001](https://attack.mitre.org/techniques/T1547/001/).

The implementation of this feature - specifically the underlying VQL query - is inspired by the built-in capability of Velociraptor to upload results of hunts (the flows) to an elastic server utilizing the [Elastic.Flows.Upload artifact](https://docs.velociraptor.app/artifact_references/pages/elastic.flows.upload/).

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
Big thanks to [Christoph Lobmeyer](https://github.com/lo-chr) who
contributed the intricate expression that is behind the `--subscribe <artifact>`
option. 🙏
:::
