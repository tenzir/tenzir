---
title: Tenzir v4.3
authors: [jachris, mavam]
date: 2023-10-10
tags: [release, operators, observability, fluent-bit, json, yaml, labels]
---

Exciting times, Tenzir v4.3 is out! The headlining feature is [Fluent
Bit][fluentbit] support with the `fluent-bit` [source][fluentbit-source] and
[sink][fluentbit-sink] operators. Imagine you can use all Fluent Bit connectors
*plus* what Tenzir already offers. What a treat!

[fluentbit]: https://fluentbit.io/
[fluentbit-source]: /next/operators/sources/fluent-bit
[fluentbit-sink]: /next/operators/sinks/fluent-bit

![Tenzir v4.3](tenzir-v4.3.excalidraw.svg)

<!--truncate-->

## Fluent Bit on Steroids

[Fluent Bit][fluentbit] is a remarkable piece of open source software that
offers observability pipelines—quite similar to Tenzir actually. That said, our
target audience is different: rather than targeting observability, we focus on
the intersection of the security and data.

By bringing the two ecosystems together, you, dear user, benefit from the union
of features. Before diving into some examples, let's briefly compare the tech.
Fluent Bit features [inputs][inputs] and [outputs][outputs] to get data in and
out of the ecosystem. These are equivalent to Tenzir's
[connectors](/connectors). Fluent Bit also has [parsers][parsers] that map to
the equally named concept of Tenzir [parsers](/formats). Fluent Bit's
[filters][filters] would be implemented as
[transformations](/operators/transformations) in Tenzir, i.e., operators that
have a non-void input and output. The diagram illustrates these relationships:

[inputs]: https://docs.fluentbit.io/manual/pipeline/inputs
[outputs]: https://docs.fluentbit.io/manual/pipeline/outputs
[parsers]: https://docs.fluentbit.io/manual/pipeline/parsers
[filters]: https://docs.fluentbit.io/manual/pipeline/filters

![Tenzir vs. Fluent Bit](tenzir-vs-fluentbit.excalidraw.svg)

It's important to note that Tenzir pipelines separate I/O and computation.
Connectors do I/O and formats are responsible for (un)structuring data. The data
paths are symmetric in that a loader ships bytes to a parser that in turn
produces events, and a printer accepts events and turns them in to bytes that
a corresponding saver sends away.

We implemented the `fluent-bit` operator as a fusion of connector and format. We
did not integrate Fluent Bit's powerful parser abstraction, as we have an
existing framework in place for that. Similarly, we did not integrate Fluent
Bit's filters, as we have a variety of transformation operators for that.

### How do I use it?

The `fluent-bit` [source][fluentbit-source] and [sink][fluentbit-sink] operator
is where the action happens. They have the following syntax:

```
fluent-bit <plugin> [<key=value>..]
```

Both operators are very similar to the `fluent-bit` command line tool, which
has the usage `fluent-bit -i <input> -o <output> -p key=value`. In Tenzir, the
source operator implies the options `-i` and the sink `-o`, so you don't have to
write them. Similarly, appending properties in the form of key-value pairs is
so common that you can omit the `-p` options.

Let's walk through some examples. Say you want to sample three values with
Fluent Bit's [`random`][random] input:

[random]: https://docs.fluentbit.io/manual/pipeline/inputs/random

```bash
tenzir 'fluent-bit random | head 3 | write json -c'
```

This prints:

```json
{"timestamp": "2023-09-23T07:56:47.957369", "message": {"rand_value": 8106944690543729752}}
{"timestamp": "2023-09-23T07:56:48.959997", "message": {"rand_value": 2072095294278847853}}
{"timestamp": "2023-09-23T07:56:49.959988", "message": {"rand_value": 5606209024700423100}}
```

Regarding the framing: [Fluent Bit's event format][event-format] produces events
in the form of an array that can have one of two possible shapes:

1. `[[TIMESTAMP, METADATA], MESSAGE]`
2. `[TIMESTAMP, MESSAGE]`

[event-format]: https://docs.fluentbit.io/manual/concepts/key-concepts#event-format

We convert this into an event record with 3 fields:

1. `timestamp`: the event timestamp
2. `metadata`: object with key-value pairs
3. `message`: arbitrary object with inferred schema

The field `metadata` is optional, as shown in the above example.

Many Fluent Bit inputs perform network I/O. Here's a TCP socket example:

```bash
# Terminal A
tenzir 'fluent-bit tcp'
# Terminal B
echo '{"foo": {"bar": 42}}' | nc 127.0.0.1 5170
```

This outputs in terminal A:

```json
{
  "timestamp": "2023-09-23T09:35:10.623745",
  "message": {
    "foo": {
      "bar": 42
    }
  }
}
```

Let's pick another input, [`opentelemetry`][opentelemetry]:

[opentelemetry]: https://docs.fluentbit.io/manual/pipeline/inputs/opentelemetry

```bash
tenzir 'fluent-bit opentelemetry'
```

This opens a socket on port 4318 that you can send now telemetry to. Instead of
`curl`, we're using our own HTTPie-like [`http`](/connectors/http) connector to
issue a POST request:

```bash
tenzir 'from http POST 127.0.0.1:4318/v1/logs resourceLogs:=[{"resource":{},"scopeLogs":[{"scope":{},"logRecords":[{"timeUnixNano":"1660296023390371588","body":{"stringValue":"{\"message\":\"dummy\"}"},"traceId":"","spanId":""}]}]}]'
```

You should then see:

```json
{
  "timestamp": "2022-08-12T09:20:24.698112",
  "message": {
    "log": {
      "message": "dummy"
    }
  }
}
```

More powerful inputs mimic other applications, like [Splunk][splunk] or
[ElasticSearch][elasticsearch]. Want Tenzir to be like Splunk via Fluent Bit?
Here you go:

[splunk]: https://docs.fluentbit.io/manual/pipeline/inputs/splunk
[elasticsearch]: https://docs.fluentbit.io/manual/pipeline/inputs/elasticsearch

```bash
tenzir 'fluent-bit splunk'
```

You just got a Splunk HEC API waiting for you at port 9880. This is one of the
most amazing things about this integration. The entire Fluent Bit connector
ecosystem is now at your fingertips!

This extends to the outputs as well. Most mundanely, you can use the `stdout`
output from Fluent Bit as follows:

```bash
tenzir 'show operators | head 3 | fluent-bit stdout'
```

This yields:

```
[0] lib.0: [[1695494117.866096973, {}], {"name"=>"batch", "source"=>false, "transformation"=>true, "sink"=>false}]
[1] lib.0: [[1695494117.866101980, {}], {"name"=>"compress", "source"=>false, "transformation"=>true, "sink"=>false}]
[2] lib.0: [[1695494117.866103887, {}], {"name"=>"decapsulate", "source"=>false, "transformation"=>true, "sink"=>false}]
```

Want to send Suricata alerts to Slack? Here is your pipeline:

```
from file --follow eve.json
| where #schema == "suricata.alert"
| fluent-bit slack webhook=https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX
```

Or send 'em to Splunk by changing the sink to:

```
fluent-bit splunk host=127.0.0.1 port=8088 tls=on tls.verify=off
```

Oh wait, Elastic? Here you go:

```
fluent-bit es host=192.168.2.3 port=9200 index=my_index type=my_type
```

Hopefully the general pattern is clear now.

Finally, there's a cool property of Fluent Bit: it's symmetric like Tenzir.
Remember how you can use ZeroMQ sockets to bridge pipelines?

```bash
# Terminal A
tenzir 'from zmq'
# Terminal B
tenzir 'show version | to zmq'
```

You can do the same with Fluent Bit's *Forward* protocol:

```bash
# Terminal A
tenzir 'fluent-bit forward'
# Terminal B
tenzir 'show version | fluent-bit forward'
```

(We'll leave it to you to do the same with Kafka.)

:::info Implementation: MsgPack vs. Arrow
Fluent Bit uses [MsgPack](https://msgpack.org/) internally, a binary version of
JSON with a slightly richer set of types. Once a Fluent Bit input onboards
data into the internal format, all operations compute on MsgPack. And before
data exits Fluent Bit, it gets converted from MsgPack to the native format of
the output.

Incidentally, Tenzir also had an optional MsgPack implementation of its data
plane. However, we dropped the MsgPack encoding and switched to [Apache
Arrow](https://arrow.apache.org) exclusively. The reason is that most of our
workloads are analytical, where a columnar representation (especially with large
batching) outperforms due data locality and the ability to tap into vectorized
computations. Moreover, our objective is to soon integrate natively with several
data tools, such as DuckDB, pandas, polars, etc.—all of which speak Arrow.
:::

Want to try it yourself? Head over to [app.tenzir.com](https://app.tenzir.com)
where you start for free and manage Tenzir nodes and run Tenzir and Fluent Bit
pipelines.

## Tidbits

Besides Fluent Bit, the team at Tenzir has been working on some other
noteworthy improvements and features that we would like to share:

### JSON Parser Improvements

We've revamped our JSON parser to be a lot faster and more accurate in type
inference.

![Tenzir v4.3 JSON Improvements](tenzir-v4.3-json-improvements.excalidraw.svg)

Schema inference now supports empty records and empty lists. Previously both
were indistinguishable from `null` values. This is best illustrated on an
example:

```json
{"foo": [], "bar": {}, "baz": "127.0.0.1"}
{"foo": [null], "bar": null, "baz": "::1"}
{"foo": null, "bar": {}, "baz": "localhost"}
```

With Tenzir v4.2, The fields `foo` and `bar` would've been dropped from the
input, and `baz` had the type `string` for all three events.

With Tenzir v4.3, `foo` is of type `list<null>`, `bar` of type `record {}`, and
baz of type `ip` for the first two events, and of type `string` for the third.

### YAML Format

The new [`yaml`](/next/formats/yaml) format supports reading and writing YAML
documents and document streams.

For example, you can now render the configuration of the current node as valid
YAML:

```
show config | write yaml
```

This yields:

```yaml
---
tenzir:
  allow-unsafe-pipelines: true
  operators:
    suricata: "shell 'suricata -r /dev/stdin --set outputs.1.eve-log.filename=/dev/stdout --set logging.outputs.0.console.enabled=no' | read suricata\n"
    zeek: "shell 'eval \"$(zkg env)\" && zeek -r - LogAscii::output_to_stdout=T JSONStreaming::disable_default_logs=T JSONStreaming::enable_log_rotation=F json-streaming-logs' | read zeek-json --no-infer\n"
...
```

Another example, perhaps just a party tricks, is converting YAML to JSON:

```bash
tenzir 'read yaml | write json' < input.json
```

### Pipeline Labels

Nodes now support setting labels for pipelines. This feature isn't yet enabled
in the app, but will be available soon for all nodes updated to v4.3 or newer.
