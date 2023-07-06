---
sidebar_position: 0
---

# Run a pipeline

:::tip What is a pipeline?
A [pipeline](../../language/pipelines.md) is a chain of operators that begins
with a [source](../../operators/sources/README.md), optionally includes
[transformations](../../operators/sinks/README.md) in the middle, and ends in a
[sink](../../operators/sinks/README.md).
:::

## In the app

You run a pipeline in the [app](../../setup-guides/use-the-app/README.md) by
writing it in the editor and hitting the *Run* button.

The following invariants apply:

1. You must start with a source
2. The browser is always the sink

The diagram below illustrates these mechanics:

![Pipeline in the Browser](pipeline-browser.excalidraw.svg)

For example, write [`version`](../../operators/sources/version.md) and click
*Run* to see a single event arrive.

## On the command line

On the [command line](../../command-line.md), run `tenzir <pipeline>` where
`<pipeline>` is the definition of the pipeline as shown in the examples.

The following invariants apply:

1. If you do not provide source, `read json` will be added
2. If you do not provide sink, `write json` will be added

The diagram below illustrates these mechanics:

![Pipeline on the command line](pipeline-cli.excalidraw.svg)

For example, run [`tenzir 'version'`](../../operators/sources/version.md) to see
a single event rendered as JSON:

<details>
<summary>Output</summary>

```json
{
  "version": "v4.0.0-rc2-34-g9197f7355e",
  "plugins": [
    {
      "name": "compaction",
      "version": "bundled"
    },
    {
      "name": "inventory",
      "version": "bundled"
    },
    {
      "name": "kafka",
      "version": "bundled"
    },
    {
      "name": "matcher",
      "version": "bundled"
    },
    {
      "name": "netflow",
      "version": "bundled"
    },
    {
      "name": "parquet",
      "version": "bundled"
    },
    {
      "name": "pcap",
      "version": "bundled"
    },
    {
      "name": "pipeline-manager",
      "version": "bundled"
    },
    {
      "name": "platform",
      "version": "bundled"
    },
    {
      "name": "web",
      "version": "bundled"
    }
  ]
}
```

</details>

You could also render it differently by passing a different
[format](../../formats.md) to the [`write`](../../operators/sinks/write.md)
operator:

```bash
tenzir 'version | write csv'
tenzir 'version | write ssv to file /tmp/version.ssv'
tenzir 'version | write parquet to file /tmp/version.parquet'
```

Instead of passing the pipeline description to the `tenzir` executable, you can
also load the definition from a file via `-f`:

```bash
tenzir -f pipeline.tql
```

This will interpret the file contents as pipeline and run it.
