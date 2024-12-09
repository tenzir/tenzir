---
sidebar_position: 9
---

# Execute Sigma rules

Tenzir supports executing [Sigma rules](https://github.com/SigmaHQ/sigma) using
the [`sigma`](../../tql2/operators/sigma.md) operator. This allows you to run
your Sigma rules in the pipeline. The operator transpiles the provided rules
into an expression, and wraps matching events into a sighting record along with
the matched rule.

Semantically, you can think of executing Sigma rules as applying the
[`where`](../../tql2/operators/where.md) operator to the input. At a high level,
the translation process looks as follows:

![Sigma Execution](sigma-execution.excalidraw.svg)

:::info pySigma Support
Unlike the legacy `sigmac` compiler that tailors a rule to specific backend,
like Elastic or Splunk, the `sigma` operator only transpiles the structural YAML
rules to produce an expression that is then used to filter a dataflow. In the
future, we would like to write a native Tenzir backend for
[pySigma](https://github.com/SigmaHQ/pySigma). Please reach out on our
[Discord](/discord) if you would like to help us with that!
:::

## Run a Sigma rule on an EVTX file

You can run a Sigma rule on any pipeline input. For example, to apply a Sigma
rule to an EVTX file, we can use the utility
[`evtx_dump`](https://github.com/omerbenamram/evtx) to convert the binary EVTX
format into JSON and then pipe it to `sigma` on the command line:

```bash
evtx_dump -o jsonl file.evtx | tenzir 'read_json | sigma "rule.yaml"'
```
