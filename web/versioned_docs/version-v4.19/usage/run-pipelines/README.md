---
sidebar_position: 0
---

# Run pipelines

You can run a [pipeline](../../pipelines.md) in the
[app](https://app.tenzir.com) or on the command line using the `tenzir` binary.

## In the app

Run a pipeline by writing typing it in the editor and hitting the *Run* button.

The following invariants apply:

1. You must start with a source
2. The browser is always the sink

The diagram below illustrates these mechanics:

![Pipeline in the Browser](pipeline-browser.excalidraw.svg)

For example, write [`version`](../../operators/version.md) and click *Run* to
see a single event arrive.

## On the command line

On the command line, run `tenzir <pipeline>` where `<pipeline>` is the
definition of the pipeline.

If the pipeline expects events as its input, an implicit `load - | read json`
will be prepended. If it expects bytes instead, only `load -` is prepended.
Likewise, if the pipeline outputs events, an implicit `write json | save -` will
be appended. If it outputs bytes instead, only `save -` is appended.

The diagram below illustrates these mechanics:

![Pipeline on the command line](pipeline-cli.excalidraw.svg)

For example, run [`tenzir 'version'`](../../operators/version.md) to
see a single event rendered as JSON:

```json
{
  "version": "v4.6.4-155-g0b75e93026",
  "major": 4,
  "minor": 6,
  "patch": 4,
  "tweak": 155
}
```

You could also render it differently by passing a different
[format](../../formats.md) to [`write`](../../operators/write.md), or by
inferring the format from the file extension:

```bash
tenzir 'version | write csv'
tenzir 'version | to /tmp/version.ssv'
tenzir 'version | to /tmp/version.parquet'
```

Instead of passing the pipeline description to the `tenzir` executable, you can
also load the definition from a file via `-f`:

```bash
tenzir -f pipeline.tql
```

This will interpret the file contents as pipeline and run it.

## As Code

In addition to running pipelines interactively, you can also deploy *pipelines as
code (PaC)*. This infrastructure-as-code-like method differs from the app-based
deployment in two ways:

1. Pipelines deployed as code always start with the Tenzir node, ensuring
   continuous operation.
2. To safeguard them, deletion via the user interface is disallowed.

Here's a an example of deploying a pipeline through your configuration:

```yaml {0} title="<prefix>/etc/tenzir/tenzir.yaml"
tenzir:
  pipelines:
    # A unique identifier for the pipeline that's used for metrics, diagnostics,
    # and API calls interacting with the pipeline.
    suricata-over-tcp:
      # An optional user-facing name for the pipeline. Defaults to the id.
      name: Import Suricata from TCP
      # An optional user-facing description of the pipeline.
      description: |
        Imports Suricata Eve JSON from the port 34343 over TCP.
      # The definition of the pipeline. Configured pipelines that fail to start
      # cause the node to fail to start.
      definition: |
        from tcp://0.0.0.0:34343 read suricata
        | import
      # Pipelines that encounter an error stop running and show an error state.
      # This option causes pipelines to automatically restart when they
      # encounter an error instead. The first restart happens immediately, and
      # subsequent restarts after the configured delay, defaulting to 1 minute.
      # The following values are valid for this option:
      # - Omit the option, or set it to null or false to disable.
      # - Set the option to true to enable with the default delay of 1 minute.
      # - Set the option to a valid duration to enable with a custom delay.
      restart-on-error: 1 minute
      # Add a list of labels that are shown in the pipeline overview page at
      # app.tenzir.com.
      labels:
        - Suricata
        - Import
      # Disable the pipeline.
      disabled: false
```
