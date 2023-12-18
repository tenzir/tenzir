---
sidebar_position: 0
---

# Run a pipeline

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
Likewise, if the pipeline outputs events, an implicit `write json | save -` will
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
