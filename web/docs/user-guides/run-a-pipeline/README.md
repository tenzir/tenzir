---
sidebar_position: 0
---

# Run a pipeline

:::tip What is a pipeline?
A [pipeline](../../pipelines.md) is a chain of operators that begins with a
[source](../../operators/sources/README.md), optionally includes
[transformations](../../operators/sinks/README.md) in the middle, and ends in a
[sink](../../operators/sinks/README.md).
:::

## In the app

You run a pipeline in the [app](https://app.tenzir.com) by writing it in the
editor and hitting the *Run* button.

The following invariants apply:

1. You must start with a source
2. The browser is always the sink

The diagram below illustrates these mechanics:

![Pipeline in the Browser](pipeline-browser.excalidraw.svg)

For example, write [`show version`](../../operators/sources/show.md) and click
*Run* to see a single event arrive.

## On the command line

On the [command line](../../command-line.md), run `tenzir <pipeline>` where
`<pipeline>` is the definition of the pipeline as shown in the examples.

If the given pipeline expects events as its input, an implicit
`load - | read json` will be prepended. If it expects bytes instead, only
`load -` is prepended. Likewise, if the pipeline outputs events, an implicit
`write json | save -` will be appended. If it outputs bytes instead, only
`save -` is appended.

The diagram below illustrates these mechanics:

![Pipeline on the command line](pipeline-cli.excalidraw.svg)

For example, run [`tenzir 'show version'`](../../operators/sources/show.md) to
see a single event rendered as JSON:

```json
{
  "version": "v4.0.0-rc12-8-g6d49fd95d1"
}
```

You could also render it differently by passing a different
[format](../../formats.md) to the [`write`](../../operators/transformations/write.md)
operator, or by inferring the format from the file extension:

```bash
tenzir 'show version | write csv'
tenzir 'show version | to file /tmp/version.ssv'
tenzir 'show version | to file /tmp/version.parquet'
```

Instead of passing the pipeline description to the `tenzir` executable, you can
also load the definition from a file via `-f`:

```bash
tenzir -f pipeline.tql
```

This will interpret the file contents as pipeline and run it.
