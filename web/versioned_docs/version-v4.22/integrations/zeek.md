# Zeek

The [Zeek](https://zeek.org) network monitor translates raw packets into
structured logs.

Tenzir supports various Zeek use cases, such as continuous ingestion, ad-hoc log
file processing, and even generating Zeek logs.

:::info Zeek Blog Post Series
We wrote several Zeek blog posts in the past that cover various aspects of the
Zeek integration in much more detail.
- [Mobilizing Zeek Logs](/blog/mobilizing-zeek-logs)
- [Zeek and Ye Shall Pipe](/blog/zeek-and-ye-shall-pipe)
- [Shell Yeah! Supercharging Zeek and Suricata with Tenzir](/blog/shell-yeah-supercharging-zeek-and-suricata-with-tenzir)
- [Native Zeek Log Rotation & Shipping](/blog/native-zeek-log-rotation-and-shipping)
- [Tenzir for Splunk Users](/blog/tenzir-for-splunk-users)
:::

Zeek logs come in [three forms](/blog/mobilizing-zeek-logs) in practice, all of
which Tenzir can parse natively:

1. [`zeek-tsv`](../formats/zeek-tsv.md): Tab-Separated Values (TSV) with a
   custom header.
2. [`zeek-json`](../formats/zeek-tsv.md): One NDJSON file for all log types
   (aka. *JSON Streaming*) including an extra `_path` and `_write_ts` field.
3. [`json`](../formats/json.md): One NDJSON file per log type.

## Ingest logs into a node

To ingest Zeek logs into a Tenzir node, you have multiple options.

### Easy-button import with the official Zeek package

Our official [Zeek package](https://github.com/tenzir/zeek-tenzir) makes it easy
to ship your Zeek logs to a Tenzir node. Install the package first:

```bash
zkg install zeek-tenzir
```

Then add this to your `$PREFIX/share/zeek/site/local.zeek` to send all logs that
Zeek produces to a Tenzir node:

```zeek
@load tenzir/import

# Uncomment to keep the original Zeek logs.
# redef Tenzir::delete_after_postprocesing=F;
```

For ad-hoc command line processing you can also pass `tenzir/import` to a Zeek
invocation:

```bash
# Ship logs to it and delete the original files.
zeek -r trace.pcap tenzir/import

# Ship logs to it and keep the original files.
zeek -r trace.pcap tenzir/import Tenzir::delete_after_postprocesing=F
```

For further details on how to use our Zeek package read our blog post [Native
Zeek Log Rotation & Shipping](/blog/native-zeek-log-rotation-and-shipping).

### Run an import pipeline when rotating logs

If you cannot use our Zeek package, it is still possible to let Zeek trigger an
import pipeline upon rotation. Zeek's [logging
framework](https://docs.zeek.org/en/master/frameworks/logging.html) can execute
a shell script whenever it rotates a log file.

This requires setting `Log::default_rotation_interval` to a non-zero value. The
default of `0 secs` means that log rotation is disabled. Add this to
`$PREFIX/share/zeek/site/local.zeek`, which is the place for local configuration
changes:

```
redef Log::default_rotation_interval = 1 day;
```

Then redefine
[`Log::default_rotation_postprocessor_cmd`](https://docs.zeek.org/en/master/scripts/base/frameworks/logging/main.zeek.html#id-Log::default_rotation_postprocessor_cmd)
to point to your shell script, e.g., `/usr/local/bin/ingest`:

```
redef Log::default_rotation_postprocessor_cmd=/usr/local/bin/ingest;
```

Here is an example `ingest` script that imports all files into a Tenzir node:

```bash title="ingest"
#!/bin/sh

file_name="$1"
base_name="$2"
from="$3"
to="$4"
terminating="$5"
writer="$6"

if [ "$writer" = "ascii" ]; then
  format="zeek-tsv"
elif [ "$writer" = "json" ]; then
  format="json --schema zeek.$base_name"
else
  echo "unsupported Zeek writer: $writer"
  exit 1
fi

pipeline="from file \"$file_name\" read $format | import"

tenzir "$pipeline"
```

Our blog post [Native Zeek Log Rotation &
Shipping](/blog/native-zeek-log-rotation-and-shipping) provides further details
on this method.

## Run Zeek on a packet pipeline

You can run Zeek on a pipeline of PCAP packets and continue processing the logs
in the same pipeline. A stock Tenzir installation comes with a
[user-defined](../language/user-defined-operators.md) `zeek` operator that looks
as follows:

```yaml title=tenzir.yaml
tenzir:
  operators:
    zeek:
      shell "eval \"$(zkg env)\" &&
             zeek -r - LogAscii::output_to_stdout=T
             JSONStreaming::disable_default_logs=T
             JSONStreaming::enable_log_rotation=F
             json-streaming-logs"
      | read zeek-json
```

This allows you run Zeek on a packet trace as follows:

```bash
tenzir 'load /path/to/trace.pcap | zeek'
```

You can also perform more elaborate packet filtering by going through the
[`pcap`](../formats/pcap.md) parser:

```bash
tenzir 'from /path/to/trace.pcap
       | decapsulate
       | where 10.0.0.0/8 || community == "1:YXWfTYEyYLKVv5Ge4WqijUnKTrM="
       | write pcap
       | zeek'
```

Read the [in-depth blog
post](/blog/shell-yeah-supercharging-zeek-and-suricata-with-tenzir) for more
details about the inner workings of the [`shell`](../operators/shell.md)
operator.

## Process logs with a pipeline on the command line

Zeek ships with a helper utility `zeek-cut` that operators on Zeek's
tab-separated logs. For example, to extract the host pairs from a conn log:

```bash
zeek-cut id.orig_h id.resp_h < conn.log
```

The list of arguments to `zeek-cut` are the column names of the log. The
[`select`](../operators/select.md) operator performs the equivalent in Tenzir
after we parse the logs as [`zeek-tsv`](../formats/zeek-tsv.md):

```bash
tenzir 'read zeek-tsv | select zeek-cut id.orig_h id.resp_h' < conn.log
```

Since pipelines are *multi-schema* and the Zeek TSV parser is aware of log
boundaries, you can also concatenate logs of various types:

```bash
cat *.log | tenzir 'read zeek-tsv | select zeek-cut id.orig_h id.resp_h'
```

## Generate Zeek TSV from arbitrary data

The [`zeek-tsv`](../formats/zeek-tsv.md) is not only a parser, but also a
printer. This means you can render any data as Zeek TSV log.

For example, print the Tenzir version as Zeek TSV log:

```bash
tenzir 'show version | write zeek-tsv'
```

This yields the following output:

```
#separator \x09
#set_separator	,
#empty_field	(empty)
#unset_field	-
#path	tenzir.version
#open	2023-12-16-08-47-12.372511
#fields	version	major	minor	patch	tweak
#types	string	count	count	count	count
v4.6.4-155-g0b75e93026	4	6	4	155
#close	2023-12-16-08-47-12.372736
```
