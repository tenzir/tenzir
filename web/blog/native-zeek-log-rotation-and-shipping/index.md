---
title: Native Zeek log Rotation & Shipping
authors: mavam
date: 2023-07-27
tags: [zeek, logs, shipping, rotation, pipelines]
comments: true
---

Did you know that [Zeek](http://zeek.org) support log rotation triggers, so that
you can do anything you want with a newly rotated batch of logs?

![Zeek Log Rotation](zeek-log-rotation.excalidraw.svg)

<!-- truncate -->

This blog post shows you how to use Zeek's native log rotation feature to
conveniently invoke any post-processor, such as a log shipper. We illustrate how
you can ingest data into Tenzir, but you can plug in any log shipper you want.

## External Log Shipping (pull)

In case you're not using Zeek's native log rotation trigger, you may observe a
directory to which Zeek periodically writes files.

Log shippers can take care of that, but your mileage may vary. For example,
[Filebeat][filebeat] works for stock Zeek only. Every log file is hard-coded. If
you have custom scripts or extend some logs, you're left alone. Filebeat also
uses the stock Zeek JSON output, which has no type information. Filebeat then
brings the typing back manually later as it converts the logs to the Elastic
Common Schema (ECS).

[filebeat]: https://www.elastic.co/guide/en/beats/filebeat/current/filebeat-module-zeek.html

## Native Log Shipping (push)

There's also a lesser known push-based option with [Zeek's logging
framework](https://docs.zeek.org/en/master/frameworks/logging.html). You can
provide a shell script that Zeek invokes *whenever it rotates file*. The shell
script receives the new cut filename as argument, plus some additional metadata.

First, to activate log rotation, you need to set
`Log::default_rotation_interval` to a non-zero value. The default of `0 secs`
means that log rotation is disabled.

Second, to customize what's happening on rotation you can is redefine
[`Log::default_rotation_postprocessor_cmd`](https://docs.zeek.org/en/master/scripts/base/frameworks/logging/main.zeek.html#id-Log::default_rotation_postprocessor_cmd)
to point to a shell script.

For example, to rotate every log files every 10 minutes with a custom `ingest`
script, you can invoke Zeek as follows:

```bash
zeek -r trace.pcap \
  Log::default_rotation_postprocessor_cmd=ingest \
  Log::default_rotation_interval=10mins
```

Let's take a look at the `ingest` shell script in more detail. Zeek always pass
6 parameters to the script:

1. The filename of the log, e.g., `/path/to/conn.log`
2. The type of the log (aka. `path`), such as `conn` or `http`.
3. Timestamp when Zeek opened the log file
4. Timestamp when Zeek closed (= rotated) the log file
5. A flag that is true when rotation occurred due to Zeek terminating
6. The format of the log, which is either `ascii` (=
   [`zeek-tsv`](/formats/zeek-tsv)) or [`json`](/formats/json)

Here's a complete example that leverages (1), (2), and (6):

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

pipeline="from file $file_name read $format | import"

tenzir "$pipeline"
```

### Flexible pipelines

When you run Zeek as above, the `ingest` script dynamically constructs the
"right" ingestion pipeline based on the type of the Zeek log at hand. The
pipelines may look like this for a `conn.log` file, based on whether you use TSV
or JSON logging:

```
from file /path/to/conn.log read zeek-tsv | import
from file /path/to/conn.log read json --schema zeek.conn | import
```

This pipeline reads the Zeek log and pipes it to the
[`import`](/operators/sinks/import) operator, which stores all your logs at a
Tenzir node. You could also use any other operator here. For example, use
[`extend`](/operators/transformations/extend) if you want to include the
filename in the data:

```bash
pipeline="from file $file_name read $format \
          | extend filename=$file_name \
          | import"
```

Take a look at the [list of operators](/operators) for further inspiration on
things you can do, or check out the [user guides](/user-guides) for concrete
ideas.

### Zeek package

If you want Tenzir post-processing out of the box, use our official [Zeek
package](https://github.com/tenzir/zeek-tenzir):

```bash
zkg install tenzir
```

After installing the package, you have two options to run pipelines on rotated
Zeek logs:

1. Load the `tenzir-import` Zeek script to ship logs to a local Tenzir node

    ```bash
    zeek -r trace.pcap tenzir-import
    ```

2. Write Zeek scripts to register pipelines manually:

   ```zeek
   # Activate log rotation by setting a non-zero value.
   redef Log::default_rotation_interval = 10 mins;
 
   event zeek_init()
     {
     Tenzir::postprocess("import");
     Tenzir::postprocess("to directory /tmp/logs write parquet");
     }
   ```

   The above Zeek script hooks up two pipelines via the function
   `Tenzir::postprocess`. Each pipelines executes upon log rotation and receives
   the Zeek log file as input. The first simply imports all data via
   [`import`](/operators/sinks/import`) and the second writes the logs as
   [`parquet`](/formats/parquet) files using [`to`](/operators/sinks/to).

## Fate sharing

Zeek implements the above desribed log rotation logic by spawning a separate
child process. When the (parent) Zeek process dies, e.g., due power loss or
running out of memory, it takes all children down along with it.

This may not be a problem for one-shot trace file analysis, but live deployments
may require higher availability and consistency. For such scenarios, we
recommend *the opposite* strategy: **do not run the post-processing within Zeek.
Decouple it instead.** One way to achieve this is by observing side effects. For
example, [zeek-archiver](https://github.com/zeek/zeek-archiver) watches a
directory for new log files that Zeek rotates in there. We have a dedicated
[roadmap item](https://github.com/tenzir/public-roadmap/issues/51) directory
watching.

Another approach is a dedciated [writer
plugin](/blog/mobilizing-zeek-logs#writer-plugin) that immediately ship logs
instead of going through the file system.

## Conclusion

This blog post shows how you can use Zeek's native log rotation feature to
invoke an arbitrary command as soon as a log file gets rotated. This approach
provides an attractive alternative that turns pull-based file monitoring into
more flexible push-based delivery. With our Zeek package, you can launch
pipelines directly from Zeek. Use it with care in live deployments, as this
implies fate sharing of your log shipping with Zeek.

Tenzir makes it easy to work with your Zeek logs. Read our [other Zeek
blogs](/blog/tags/zeek) and [try it](/get-started) yourself.
