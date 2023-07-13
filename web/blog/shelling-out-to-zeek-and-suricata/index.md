---
title: Shelling out to Zeek and Suricata
authors: mavam
date: 2023-07-20
tags: [zeek, suricata, logs, shell]
---

As an incident responder, threat hunter, or detection engineer, getting quickly
to your analytics is key for productivity. For network-based visibility and
detection, [Zeek](https://zeek.org) and [Suricata](https://suricata.io) are the
bedrock for many security teams. But operationalizing these tools can take a
good chunk of time.

So we asked ourselves: **How can we make it super easy to work with Zeek and
Suricata logs?**

![Shell Operator](shell-operator.excalidraw.svg)

<!--truncate-->

## Purring like a Suricat

In our [previous blog post](/blog/zeek-and-ye-shall-pipe/) we adapted Zeek to
behave like a good ol' Unix tool, taking input via stdin and producing output
via stdout. Turns out you can do the same fudgery with Suricata:[^1]

[^1]: Suricata outputs [EVE
JSON](https://suricata.readthedocs.io/en/latest/output/eve/eve-json-output.html)
by default, which is equivalent to Zeek's streaming JSON output, with the
difference being that Zeek's `_path` field is called `event_type` in Suricata
logs.

```bash title=suricatify
#!/bin/sh
suricata -r /dev/stdin \
  --set outputs.1.eve-log.filename=/dev/stdout \
  --set logging.outputs.0.console.enabled=no
```

Let's break this down:

- The `--set` option take a `name=value` parameter that overrides the settings
  in your `suricata.yaml` config file.
- The key `outputs.1.eve-log.filename` refers to the `outputs` array, takes
  element at index `1`, treats that as object and goes to the nested field
  `eve-log.filename`. Setting `/dev/stdout` as filename makes Suricata write to
  stdout.
- We must set `logging.outputs.0.console.enabled` to `no` because Suricata
  writes startup log messages to stdout. Since they are not valid JSON, we
  would otherwise create an invalid JSON output stream.

## User-defined Operators

Now that we have both Zeek and Suricata at our fingertips, how can we work with
their output more easily? This is where Tenzir comes into play—easy
[pipelines](/docs/language/pipelines) for security teams to acquire,
[reshape](/docs/user-guides/get-started), and route event data.

For our use case we look at the [`shell`](/docs/operators/shell) operator in
more detail: it lifts any Unix command into a pipeline. Here are two examples
that rely on our two shell scripts:

```bash
# Zeek
zcat pcap.gz | zeekify | tenzir \
  'read zeek-json
   | where #schema == "zeek.conn"
   | summarize n=count_distinct(id.orig_h) by id.resp_h
   | sort n desc'
# Suricata
zcat pcap.gz | suricatify | tenzir \
  'read suricata
   | where #schema == "suricata.flow"
   | summarize n=count_distinct(src_ip) by dst_ip
   | sort n desc'
```

The two aggregations actually do the same thing: count the number of unique
source IP addresses per destination IP address.

It's a bit unwiedly to write such a command line that requires an external shell
script to work. This is where [user-defined operators](/operators/user-defined)
come into play. You can write a custom `zeek` and `suricata` operator and ditch
the shell script:

```yaml title="tenzir.yaml"
tenzir:
  operators:
    zeek: >
     shell "zeek -r - LogAscii::output_to_stdout=T \
            JSONStreaming::disable_default_logs=T \
            JSONStreaming::enable_log_rotation=F \
            json-streaming-logs"
     | parse zeek-json
    suricata: >
     shell "suricata -r /dev/stdin \
            --set outputs.1.eve-log.filename=/dev/stdout \
            --set logging.outputs.0.console.enabled=no"
     | parse suricata
```

The difference stands out when you look now at the pipeline definition:

```text title=Zeek
zeek
| where #schema == "zeek.conn"
| summarize n=count_distinct(id.orig_h) by id.resp_h
| sort n desc
```

```text bash title=Suricata
suricata
| where #schema == "suricata.flow"
| summarize n=count_distinct(src_ip) by dst_ip
| sort n desc
```

It's pretty convenient to drop packets into a Tenzir pipeline, process them with
our favorite tools, and then perform fast in-situ analytics on them. The nice
thing is that operators compose: a new operator automatically works with all
existing ones.

## How does it work?

Under the hood, the `shell` operator forks the `tenzir` process and spawns
`zeek` or `suricata` as child process. The operator then rewires stdin and
stdout of the `tenzir` process such their byte streams get routed through stdin
and stdout of the forked child.

Compare this to the "standard" approach:

![Piping Zeek to Tenzir](zeek-to-tenzir-pipe.excalidraw.svg)

Using `shell`, we have this scenario:

![Shelling out to Zeek](zeek-to-tenzir-shell.excalidraw.svg)

The `shell` operator is bytes-to-bytes transformation. This means it can only
interact with other operators that also work on bytes. In the above examples, we
used the shell pipe to connect stdin and stdout of two processes. We can also
use Tenzir's native pipe with the [`file`](/docs/connectors/file) loader:

```
load file trace.pcap
| zeek
| where 6.6.6.6
| write json
```

Got a PCAP trace via Kafka? Just exchange the `file` loader with
[`kafka`](/docs/connectors/kafka):

```
load kafka -t artifact
| zeek
| where 6.6.6.6
| write json
```

You may not always sit in front of a command line and are able to pipe data from
a Unix tool into a Tenzir pipeline. For example, when you use our
[app](/setup-guides/use-the-app) or the [REST API](/rest-api). This is where the
`shell` operator shines. The diagram above shows how `shell` shifts the entry
point of data from a tool to the Tenzir process. You can consider `shell` your
escape hatch to reach deeper into a specific Tenzir node, as if you had a native
shell.

:::info Unsafe operators
Naturally, we must add that this is also a security risk and can lead to
arbitrary shell access. This is why the `shell` operator and a few other ones
with far-reaching side effects are disabled by default. If you are aware of the
implications, you can remove this restriction by setting
`tenzir.allow-unsafe-pipelines: true` in the `tenzir.yaml` of the respective
node.
:::

## Conclusion

In this blog post we showed you the [`shell`](/operators/sources/shell) operator
and how you can use it to integrate third-party tooling into a Tenzir pipeline
when coupled with [user-defined operators](/operators/user-defined).
