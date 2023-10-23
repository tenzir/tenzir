---
title: Matching Yara Rules in Byte Pipelines
authors:
   - mavam
date: 2023-10-24
tags: [yara, operator, dfir, detection engineering]
comments: true
---

The new [`yara`][yara-operator] operator matches [YARA][yara] rules on byte
pipelines, producing a structured match output to conveniently integrate
alerting tools or trigger next processing steps in your detection workflows.

[yara]: https://virustotal.github.io/yara/
[yara-operator]: /next/operators/transformations/yara

![Yara Operator](yara-operator.excalidraw.svg)

<!--truncate-->

[YARA][yara] is one a bedrock tool when it comes to writing detections on binary
data. Malware analysts develop them based on sandbox results or threat reports,
incident responders capture the attacker's toolchain on disk images or in
memory, and security engineers share them with their peers.

## Operationalizing YARA rules

The most straight-forward way to execute a YARA rule is the official [`yara`
command-line utility](https://yara.readthedocs.io/en/stable/commandline.html).
Consider this rule:

```
rule test {
  meta:
    string = "string meta data"
    integer = 42
    boolean = true

  strings:
    $foo = "foo"
    $bar = "bar"

  condition:
    $foo and $bar
}
```

Running `yara -g -e -s -L test.yara test.txt` on a file `test.txt` with contents
`foo barbar baz` yields the following output:

```
default:test [] test.txt
0x0:3:$foo: foo
0x4:3:$bar: bar
0x7:3:$bar: bar
```

There are other ways to operationalize YARA rules, e.g., the
[ClamAV](https://www.clamav.net/) scanner,
[osquery](https://osquery.readthedocs.io/en/stable/deployment/yara/), or
[Velociraptor](https://docs.velociraptor.app/vql_reference/plugin/yara/)—which
we also [integrated as pipeline
operator](/blog/integrating-velociraptor-into-tenzir-pipelines).

## The `yara` operator

Our new [`yara`](/operators/transformations/yara) operator is a transformation
that accepts bytes as input and produces events as output. Let's take the simple
case of running the above example on string input:

```bash
echo 'foo barbar baz' |
  tenzir 'load stdin | yara /tmp/test.yara'
```

The resulting `yara.match` events look as follows:

```json
{
  "rule": {
    "identifier": "test",
    "namespace": "default",
    "string": "foo",
    "tags": [],
    "meta": [
      {
        "key": "string",
        "value": "\"string meta data\""
      },
      {
        "key": "integer",
        "value": "42"
      },
      {
        "key": "boolean",
        "value": "true"
      }
    ],
    "matches": [
      {
        "identifier": "$foo",
        "data": "foo",
        "base": 0,
        "offset": 0,
        "match_length": 3
      }
    ]
  }
}
{
  "rule": {
    "identifier": "test",
    "namespace": "default",
    "string": "bar",
    "tags": [],
    "meta": [
      {
        "key": "string",
        "value": "\"string meta data\""
      },
      {
        "key": "integer",
        "value": "42"
      },
      {
        "key": "boolean",
        "value": "true"
      }
    ],
    "matches": [
      {
        "identifier": "$bar",
        "data": "bar",
        "base": 0,
        "offset": 4,
        "match_length": 3
      },
      {
        "identifier": "$bar",
        "data": "bar",
        "base": 0,
        "offset": 7,
        "match_length": 3
      }
    ]
  }
}
```

Each event represents a rule match on a rule string. The output has a `rule`
field that describes the metadata of the rule, and a `matches` array with a list
of all matches for a given rule string. Each match includes the string
`identifier`, e.g., `$foo`, the matching data, and details about the location of
the match.

## Mixing and matching loaders

The [`stdin`](/connectors/stdin) loader in the above example produces chunks of
bytes. But you can use any connector of your choice that yields bytes. In
particular, you can use the [`file`](/connectors/file) loader:

```bash
tenzir 'load file --mmap /tmp/test.txt | yara /tmp/test.yara'
```

Note the `--mmap` flag: we need it to deliver a single block of data to the
`yara` operator. Without `--mmap`, the `file` loader will generate a stream of
byte chunks and feed them incrementally to the `yara` operator, attempting to
match `rule.yara` individually on *every chunk*. This will cause false negatives
when rule matches span chunk boundaries.

If you have a Kafka topic where you publish malware samples to be scanned, then
you only need to change the pipeline source:

```bash
tenzir 'load kafka --topic malware-samples | yara /tmp/test.yara'
```

This is where the [separation between structured and unstructured
data][separation-of-concerns] in our pipelines pays off. You plug in any loader
while leaving the remainder of `yara` pipeline in place.

[separation-of-concerns]: /blog/five-design-principles-for-building-a-data-pipeline-engine#p1-separation-of-concerns

## Post-processing matches

Because the matches are structured events, you can use all existing operators to
post-process them. For example, send them to a Slack channel via Fluent Bit:

```
load file --mmap /tmp/test.txt
| yara /tmp/test.yara
| fluent-bit slack webhook=URL
```

Or store them with [`import`](/operators/sinks/import) at a Tenzir node to
generate match statistics later on:

```
load file --mmap /tmp/test.txt
| yara /tmp/test.yara
| import
```

## Summary

We've introduced the [`yara`][yara-operator] operator as a byte-to-events
transformation that exposes YARA rule matches as structured events, making them
easy to post-process with the existing collection of Tenzir operators.

Try it yourself. Deploy detection pipelines with the `yara` operator for free
with our Community Edition at [app.tenzir.com](https://app.tenzir.com). Missing
any other operators that operationalize detections? Swing by our [Discord
server](/discord) and let us know!
