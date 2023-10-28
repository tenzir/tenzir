---
title: Matching YARA Rules in Byte Pipelines
authors:
   - mavam
date: 2023-11-02
tags: [yara, operator, dfir, detection engineering]
comments: true
---

The new [`yara`][yara-operator] operator matches [YARA][yara] rules on bytes,
producing a structured match output to conveniently integrate alerting tools or
trigger next processing steps in your detection workflows.

[yara]: https://virustotal.github.io/yara/
[yara-operator]: /next/operators/transformations/yara

![YARA Operator](yara-operator.excalidraw.svg)

<!--truncate-->

[YARA][yara] is one a bedrock tool when it comes to writing detections on binary
data. Malware analysts develop them based on sandbox results or threat reports,
incident responders capture the attacker's toolchain on disk images or in
memory, and security engineers share them with their peers.

## Operationalize YARA rules

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

There are other ways to execute YARA rules, e.g.,
[ClamAV](https://www.clamav.net/),
[osquery](https://osquery.readthedocs.io/en/stable/deployment/yara/), or
[Velociraptor](https://docs.velociraptor.app/vql_reference/plugin/yara/)—which
we also [integrated as pipeline
operator](/blog/integrating-velociraptor-into-tenzir-pipelines).

And now there's also Tenzir, with a [`yara`][yara-operator] operator that
accepts bytes as input and produces events as output. Let's take the simple case
of running the above example on string input:

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
    "meta": {
      "string": "string meta data",
      "integer": 42,
      "boolean": true
    },
    "matches": [
      {
        "identifier": "$foo",
        "data": "Zm9v",
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
    "meta": {
      "string": "string meta data",
      "integer": 42,
      "boolean": true
    },
    "matches": [
      {
        "identifier": "$bar",
        "data": "YmFy",
        "base": 0,
        "offset": 4,
        "match_length": 3
      },
      {
        "identifier": "$bar",
        "data": "YmFy",
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
`identifier`, e.g., `$foo`, an excerpt of the matching data in field `data` as
blob,[^1] and details about the location of the match.

[^1]: JSON doesn't distinguish binary blobs from strings. However, our type
    system does, so we encode blob values as Base64-encoded strings for formats
    that do not have a native blog representation.

## Mix and match loaders

The [`stdin`](/connectors/stdin) loader in the above example produces chunks of
bytes. But you can use any connector of your choice that yields bytes. In
particular, you can use the [`file`](/connectors/file) loader:

```bash
tenzir 'load file --mmap /tmp/test.txt | yara /tmp/test.yara'
```

If you have a [ZeroMQ socket](/connectors/zmq) where you publish malware samples
to be scanned, then you only need to change the pipeline source:

```bash
tenzir 'load zmq | yara /tmp/test.yara'
```

This is where the [separation between structured and unstructured
data][separation-of-concerns] in pipelines pays off. You plug in any loader
while leaving the remainder of `yara` pipeline in place.

[separation-of-concerns]: /blog/five-design-principles-for-building-a-data-pipeline-engine#p1-separation-of-concerns

## Post-process matches

Because the matches are structured events, you can use all existing operators to
post-process them. For example, send them to a Slack channel via
[`fluent-bit`](/operators/sinks/fluent-bit):

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

## Create a YARA rule matching service

Using just a few pipelines, you can quickly deploy a YARA rule scanning service
that sends the matches to a Slack webhook. Let's that you want to scan malware
sample that you receive over a [Kafka](../../connectors/kafka) topic
`malware`. Launch the processing pipeline as follows:

```
load kafka --topic malware
| yara --blockwise /path/to/rules
| fluent-bit slack webhook=URL
```

This pipeline requires that every Kafka message is a self-contained malware
sample. Because the pipeline runs continuously, we supply the `--blockwise`
option so that the `yara` triggers a scan for every Kafka message, as opposed to
accumulating all messages indefinitely and only initiating a scan when the input
exhausts.

You can now submit a malware sample by sending it to the `malware` Kafka topic:

```
load file --mmap evil.exe | save kafka --topic malware
```

The matches should now arrive as JSON message in the Slack channel associated
with the webhook.

## Summary

We've introduced the [`yara`][yara-operator] operator as a byte-to-events
transformation that exposes YARA rule matches as structured events, making them
easy to post-process with the existing collection of Tenzir operators. We also
explained how you can create a simple YARA rule scanning service that accepts
malware samples via Kafka and sends the matches to a Slack channel.

Try it yourself. Deploy detection pipelines with the `yara` operator for free
with our Community Edition at [app.tenzir.com](https://app.tenzir.com). Missing
any other operators that operationalize detections? Swing by our [Discord
server](/discord) and let us know!

:::note Acknowledgements
Thanks to [Thomas Patzke](https://github.com/thomaspatzke) for reviewing this
blog post and suggesting to make the default behavior of the operator more safe
to use. 🙏
:::
