---
sidebar_position: 0
---

# Run a pipeline

A [pipeline](../language/pipelines.md) is a chain of operators that begins with
a [source](../operators/sources/README.md), optionally includes
[transformations](../operators/sinks/README.md) in the middle, and ends in a
[sink](../operators/sinks/README.md). Use the `tenzir` executable to run a
pipeline:

```bash
tenzir 'version | write json'
```

The [`version`](../operators/sources/version.md) operator is a source that emits
a single event (the Tenzir versions) and the
[`write`](../operators/sinks/write.md) operator a sink that takes a
[format](../formats.md) as argument. This invocation prints:

```json
{"version": "v3.1.0-377-ga790da3049-dirty", "plugins": [{"name": "parquet", "version": "bundled"}, {"name": "pcap", "version": "bundled"}, {"name": "sigma", "version": "bundled"}, {"name": "web", "version": "bundled"}]}
```

## Run from file

Instead of passing the pipeline description to the `tenzir` executable, you can
also load the definition from a file via `-f`:

```bash
tenzir -f pipeline.tql
```

This will interpret the file contents as pipeline and run it.

:::tip Easy-Button Pipelines
Want it managed? Head to [tenzir.com](https://tenzir.com) and sign up for the
free Community Edition to experience easy-button pipeline management from the
browser.
:::
