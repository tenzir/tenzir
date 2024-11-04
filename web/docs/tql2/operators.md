# Operators

import './operators.css';

Tenzir comes with a wide range of built-in pipeline operators.

## Modify

Operator | Description | Example
:--------|:------------|:-------
[`set`](./operators/set.md) | Assigns a value to a field, creating it if necessary | `name = "Tenzir"`
[`select`](./operators/select.md) | Selects some values and discard the rest | `select name, id=metadata.id`
[`drop`](./operators/drop.md) | Removes fields from the event | `drop name, metadata.id`
[`enumerate`](./operators/enumerate.md) | Adds a field with the number of the event | `enumerate num`

<!--
TODO: Do we want to document set, and if so, how?
[`unroll`]() | … | `…`
[`yield`]() | … | `…`
-->

## Filter

Operator | Description | Example
:--------|:------------|:-------
[`where`](./operators/where.md) | Keeps only events matching a predicate | `where name.starts_with("John")`
[`assert`](./operators/assert.md) | Same as `where`, but warns if predicate is `false` | `assert name.starts_with("John")`
[`taste`](./operators/taste.md) | Keeps only N events of each type | `taste 1`
[`head`](./operators/head.md) | Keeps only the first N events | `head 20`
[`tail`](./operators/tail.md) | Keeps only the last N events | `tail 20`
[`slice`](./operators/slice.md) | Keeps a range of events with an optional stride | `slice begin=10, end=30`

<!--
[`deduplicate`]() | … | `…`
-->

## Analyze

Operator | Description | Example
:--------|:------------|:-------
[`summarize`](./operators/summarize.md) | Aggregates events with implicit grouping | `summarize name, sum(transaction)`
[`sort`](./operators/sort.md) | Sorts the events by one or more expressions | `sort name, -abs(transaction)`
[`reverse`](./operators/reverse.md) | Reverses the event order | `reverse`
[`top`](./operators/top.md) | Shows the most common values | `top user`
[`rare`](./operators/rare.md) | Shows the least common values | `rare auth.token`

## Flow Control

Operator | Description | Example
:--------|:------------|:-------
[`every`](./operators/every.md) | Restarts a pipeline periodically | `every 10s { summarize sum(transaction) }`
[`fork`](./operators/fork.md) | Forwards a copy of the events to another pipeline | `fork { to "copy.json" }`
[`if`](language/statements.md#if) | Splits the flow based on a predicate | `if transaction > 0 { … } else { … }`

<!--
[`group`]() | Starts a new pipeline for each group | `group path { to $path }`
[`timeout`]() | Ends a pipeline after a period without input | `timeout 1s { … }`
[`match`]() | Splits the flow with pattern matching | `match name { "Tenzir" => {…}, _ => {…} }`
-->

## Input

Operator | Description | Example
:--------|:------------|:-------
[`diagnostics`](./operators/diagnostics.md) | Retrieves diagnostic events of managed pipelines | `diagnostics`
[`export`](./operators/export.md) | Retrieves events from the node | `export`
[`load_file`](./operators/load_file.md) | Loads bytes from a file | `load_file "/tmp/data.json"`
[`load_google…`](./operators/load_google_cloud_pubsub.md) | Listen to a Google Cloud Pub/Sub subscription | `load_google_cloud_pubsub "…", "…"`
[`load_http`](./operators/load_http.md) | Receives bytes from a HTTP request | `load_http "example.org", params={n: 5}`
[`load_tcp`](./operators/load_tcp.md) | Loads bytes from a TCP or TLS connection | `load_tcp "0.0.0.0:8090" { read_json }`
[`velociraptor`](./operators/velociraptor.md) | Returns results from a Velociraptor server | `velociraptor subscribe="Windows"`
[`metrics`](./operators/metrics.md) | Retrieves metrics events from a Tenzir node | `metrics "cpu"`
[`subscribe`](./operators/subscribe.md) | Subscribes to events of a certain topic | `subscribe "topic"`

<!--
[`load`](./operators/load.md) | Load bytes according to a URL | `load "https://example.org/api/list"`
[`from`](./operators/from.md) | | `from "/tmp/data.json"`
-->

## Output

Operator | Description | Example
:--------|:------------|:-------
[`azure_log_analytics`](./operators/azure_log_analytics.md) | Sends events to Azure Log Analytics | `azure_log_analytics tenant_id=…`
[`discard`](./operators/discard.md) | Discards incoming bytes or events | `discard`
[`save_file`](./operators/save_file.md) | Saves incoming bytes into a file | `save_file "/tmp/out.json"`
[`save_google_cloud…`](./operators/save_google_cloud_pubsub.md) | Publishes to a Google Cloud Pub/Sub topic | `save_google_cloud_pubsub "…", "…"`
[`save_http`](./operators/save_http.md) | Sends incoming bytes over a HTTP connection | `save_http "example.org/api"`
[`serve`](./operators/serve.md) | Makes events available at `/serve` | `serve "abcde12345"`
[`to_hive`](./operators/to_hive.md) | Writes events using hive partitioning | `to_hive "s3://…", partition_by=[x]`
[`to_splunk`](./operators/to_splunk.md) | Sends incoming events to a Splunk HEC | `to_splunk "https://localhost:8088", …`
[`import`](./operators/import.md) | Stores events at the node | `import`
[`publish`](./operators/publish.md) | Publishes events to a certain topic | `publish "topic"`

<!---
[`save`](./operators/save.md) | Save incoming bytes according to a URL | `save "https://example.org/api"`
-->

## Parsing

Operator | Description | Example
:--------|:------------|:-------
[`read_bitz`](./operators/read_bitz.md) | Parses Tenzir's internal wire format | `read_bitz`
[`read_cef`](./operators/read_cef.md) | Parses the Common Event Format | `read_cef`
[`read_csv`](./operators/read_csv.md) | Parses comma-separated values | `read_csv null_value="-"`
[`read_gelf`](./operators/read_gelf.md) | Parses the Graylog Extended Log Format | `read_gelf`
[`read_grok`](./operators/read_grok.md) | Parses events using a Grok pattern | `read_grok "%{IP:client} %{WORD:action}"`
[`read_json`](./operators/read_json.md) | Parses JSON objects | `read_json arrays_of_objects=true`
[`read_kv`](./operators/read_kv.md) | Parses key-value pairs | `read_kv r"(\s+)[A-Z_]+:", r":\s*"`
[`read_leef`](./operators/read_leef.md) | Parses the Log Event Extended Format | `read_leef`
[`read_lines`](./operators/read_lines.md) | Parses each line into a separate event | `read_lines`
[`read_ndjson`](./operators/read_ndjson.md) | Parses newline-delimited JSON | `read_ndjson`
[`read_ssv`](./operators/read_ssv.md) | Parses space-separated values | `read_ssv header="name count"`
[`read_suricata`](./operators/read_suricata.md) | Parses Suricata's Eve format | `read_suricata`
[`read_syslog`](./operators/read_syslog.md) | Parses syslog | `read_syslog`
[`read_tsv`](./operators/read_tsv.md) | Parses tab-separated values | `read_tsv auto_expand=true`
[`read_xsv`](./operators/read_xsv.md) | Parses custom-separated values | `read_xsv ";", ":", "N/A"`
[`read_yaml`](./operators/read_yaml.md) | Parses YAML | `read_yaml`
[`read_zeek_json`](./operators/read_zeek_json.md) | Parses Zeek JSON | `read_zeek_json`
[`read_zeek_tsv`](./operators/read_zeek_tsv.md) | Parses Zeek TSV | `read_zeek_tsv`

## Printing

Operator | Description | Example
:--------|:------------|:-------
[`write_bitz`](./operators/write_bitz.md) | Writes events as Tenzir's internal wire format | `write_bitz`
[`write_json`](./operators/write_json.md) | Writes events as JSON | `write_json ndjson=true`

<!--
## Packages

Operator | Description | Example
:--------|:------------|:-------
[`package::add`]() | |
[`package::remove`]() | |
[`package::list`]() | |

## Contexts

Operator | Description | Example
:--------|:------------|:-------
[`context::create`]() | |
[`context::delete`]() | |
[`context::update`]() | |
[`context::lookup`]() | |

## Charts

Operator | Description | Example
:--------|:------------|:-------
[`bar_chart`]() | All of this is TBD. |
[`treemap`]() | |
[`line_chart`]() | |
-->

## Internals

Operator | Description | Example
:--------|:------------|:-------
[`api`](./operators/api.md) | Calls Tenzir's REST API from a pipeline | `api "/pipeline/list"`
[`batch`](./operators/batch.md) | Controls the batch size of events | `batch timeout=1s`
[`buffer`](./operators/buffer.md) | Adds additional buffering to handle spikes | `buffer 10M, policy="drop"`
[`measure`](./operators/measure.md) | Returns events describing the incoming batches | `measure`
[`throttle`](./operators/throttle.md) | Limits the amount of data flowing through | `throttle 100M, within=1min`
[`cache`](./operators/cache.md) | In-memory cache shared between pipelines | `cache "w01wyhTZm3", ttl=10min`
[`legacy`](./operators/legacy.md) | Provides a compatibility fallback to TQL1 pipelines | `legacy "chart area"`

## Node Inspection

Operator | Description | Example
:--------|:------------|:-------
[`config`](./operators/config.md) | Returns the node's configuration | `config`
[`fields`](./operators/fields.md) | Lists all fields stored at the node | `fields`
[`openapi`](./operators/openapi.md) | Returns the OpenAPI specification | `openapi`
[`partitions`](./operators/partitions.md) | Retrieves metadata about events stored at the node | `partitions src_ip == 1.2.3.4`
[`plugins`](./operators/plugins.md) | Lists available plugins | `plugins`
[`schemas`](./operators/schemas.md) | Lists schemas for events stored at the node | `schemas`
[`version`](./operators/version.md) | Shows the current version | `version`

## Host Inspection

Operator | Description | Example
:--------|:------------|:-------
[`files`](./operators/files.md) | Lists files in a directory | `files "/var/log/", recurse=true`
[`nics`](./operators/nics.md) | Lists available network interfaces | `nics`
[`processes`](./operators/processes.md) | Lists running processes | `processes`
[`sockets`](./operators/sockets.md) | Lists open sockets | `sockets`

## Uncategorized

Operator | Description | Example
:--------|:------------|:-------
[`compress`](./operators/compress.md) | Compresses a stream of bytes | `compress "zstd", level=18`
[`decompress`](./operators/decompress.md) | Decompresses a stream of bytes | `decompress "brotli"`
[`delay`](./operators/delay.md) | Delays events relative to a start time | `delay ts, speed=2.5`
[`pass`](./operators/pass.md) | Does nothing with the input | `pass`
[`repeat`](./operators/repeat.md) | Repeats the input after it has finished | `repeat 100`
[`sigma`](./operators/sigma.md) | Matches incoming events against Sigma rules | `sigma "/tmp/rules/"`
[`timeshift`](./operators/timeshift.md) | Adjusts timestamps relative to a given start time | `timeshift ts, start=2020-01-01`
[`yara`](./operators/yara.md) | Matches the incoming byte stream against YARA rules | `yara "/path/to/rules", blockwise=true`
[`python`](./operators/python.md) | Executes a Python snippet for each event | `python "self.x = self.y"`
[`shell`](./operators/shell.md) | Runs a shell command within the pipeline | <code>shell "./process.sh \| tee copy.txt"</code>
