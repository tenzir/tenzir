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
[`timeshift`](./operators/timeshift.md) | Adjusts timestamps relative to a given start time | `timeshift ts, start=2020-01-01`
[`unroll`](./operators/unroll.md) | Unrolls a field of type list, duplicating the surrounding event | `unroll names` |

## Filter

Operator | Description | Example
:--------|:------------|:-------
[`where`](./operators/where.md) | Keeps only events matching a predicate | `where name.starts_with("John")`
[`assert`](./operators/assert.md) | Same as `where`, but warns if predicate is `false` | `assert name.starts_with("John")`
[`taste`](./operators/taste.md) | Keeps only N events of each type | `taste 1`
[`head`](./operators/head.md) | Keeps only the first N events | `head 20`
[`tail`](./operators/tail.md) | Keeps only the last N events | `tail 20`
[`slice`](./operators/slice.md) | Keeps a range of events with an optional stride | `slice begin=10, end=30`
[`sample`](./operators/sample.md) | Samples events based on load | `sample 30s, max_samples=2k`
[`deduplicate`](./operators/deduplicate.md) | Removes duplicate events | `deduplicate src_ip`

## Analyze

Operator | Description | Example
:--------|:------------|:-------
[`summarize`](./operators/summarize.md) | Aggregates events with implicit grouping | `summarize name, sum(amount)`
[`sort`](./operators/sort.md) | Sorts the events by one or more expressions | `sort name, -abs(transaction)`
[`reverse`](./operators/reverse.md) | Reverses the event order | `reverse`
[`top`](./operators/top.md) | Shows the most common values | `top user`
[`rare`](./operators/rare.md) | Shows the least common values | `rare auth.token`

## Flow Control

Operator | Description | Example
:--------|:------------|:-------
[`delay`](./operators/delay.md) | Delays events relative to a start time | `delay ts, speed=2.5`
[`cron`](./operators/cron.md) | Runs a pipeline periodically with a cron expression | `cron "* */10 * * * MON-FRI" { from "https://example.org" }`
[`discard`](./operators/discard.md) | Discards incoming bytes or events | `discard`
[`every`](./operators/every.md) | Runs a pipeline periodically at a fixed interval | `every 10s { summarize sum(amount) }`
[`fork`](./operators/fork.md) | Forwards a copy of the events to another pipeline | `fork { to "copy.json" }`
[`load_balance`](./operators/load_balance.md) | Routes the data to one of multiple subpipelines | `load_balance $over { publish $over }`
[`pass`](./operators/pass.md) | Does nothing with the input | `pass`
[`repeat`](./operators/repeat.md) | Repeats the input after it has finished | `repeat 100`
[`throttle`](./operators/throttle.md) | Limits the amount of data flowing through | `throttle 100M, within=1min`

<!--
[`group`]() | Starts a new pipeline for each group | `group path { to $path }`
[`timeout`]() | Ends a pipeline after a period without input | `timeout 1s { … }`
[`match`]() | Splits the flow with pattern matching | `match name { "Tenzir" => {…}, _ => {…} }`
-->

## Inputs

#### Events

Operator | Description | Example
:--------|:------------|:-------
[`from`](./operators/from.md) | Reads events from an URI<br/>Creates events from records | `from "http://example.org/file.csv.gz"`<br/>`from {key: "value"}…` <!--at the top because its important-->
[`from_fluent_bit`](./operators/from_fluent_bit.mdx) | Returns results from Fluent Bit | `from_fluent_bit "opentelemetry"`
[`from_velocira…`](./operators/from_velociraptor.md) | Returns results from a Velociraptor server | `from_velociraptor subscribe="Windows"`

#### Bytes

Operator | Description | Example
:--------|:------------|:-------
[`load_amqp`](./operators/load_amqp.md) | Loads bytes from an AMQP server | `load_amqp`
[`load_azure_blob…`](./operators/load_azure_blob_storage.md) | Load bytes from an Azure Blob Storage | `load_azure_blob_storage "abfs://…`
[`load_file`](./operators/load_file.md) | Loads bytes from a file | `load_file "/tmp/data.json"`
[`load_ftp`](./operators/load_ftp.md) | Loads bytes via FTP | `load_ftp "ftp.example.org"`
[`load_google_c…`](./operators/load_google_cloud_pubsub.md) | Listen to a Google Cloud Pub/Sub subscription | `load_google_cloud_pubsub project_id=…`
[`load_http`](./operators/load_http.md) | Receives bytes from a HTTP request | `load_http "example.org", params={n: 5}`
[`load_kafka`](./operators/load_kafka.md) | Receives bytes from an Apache Kafka topic | `load_kafka topic="example"`
[`load_nic`](./operators/load_nic.md) | Receives bytes from a Network Interface Card | `load_nic "eth0"`
[`load_s3`](./operators/load_s3.md) | Receives bytes from an Amazon S3 object | `load_s3 "s3://my-bucket/obj.csv"`
[`load_stdin`](./operators/load_stdin.md) | Receives bytes standard input | `load_stdin`
[`load_sqs`](./operators/load_sqs.md) | Receives bytes from an Amazon SQS queue | `load_sqs "sqs://tenzir"`
[`load_tcp`](./operators/load_tcp.md) | Loads bytes from a TCP or TLS connection | `load_tcp "0.0.0.0:8090" { read_json }`
[`load_udp`](./operators/load_udp.md) | Loads bytes from a UDP socket | `load_udp "0.0.0.0:8090"`
[`load_zmq`](./operators/load_zmq.md) | Receives bytes from ZeroMQ messages | `load_zmq`

## Outputs

#### Events

Operator | Description | Example
:--------|:------------|:-------
[`to`](./operators/to.md) | Writes events to an URI | `to "s3://examplebucket/obj.json.gz"` <!--at the top because its important-->
[`to_asl`](./operators/to_asl.md) | Sends OCSF events to an Amazon Security Lake | `to_asl "s3://…"`
[`to_azure_log_ana…`](./operators/to_azure_log_analytics.md) | Sends events to Azure Log Analytics | `to_azure_log_analytics tenant_id=…`
[`to_clickhouse`](./operators/to_clickhouse.md) | Sends events to a ClickHouse Table | `to_clickhouse table="my_table"`
[`to_fluent_bit`](./operators/to_fluent_bit.md) | Sends events to Fluent Bit| `to_fluent_bit "elasticsearch" …`
[`to_hive`](./operators/to_hive.md) | Writes events using hive partitioning | `to_hive "s3://…", partition_by=[x]`
[`to_opensearch`](./operators/to_opensearch.md) | Sends incoming events to the OpenSearch Bulk API | `to_opensearch 'localhost:9200", …`
[`to_snowflake`](./operators/to_snowflake.md) | Sends incoming events to a Snowflake database | `to_snowflake account_identifier="…`
[`to_splunk`](./operators/to_splunk.md) | Sends incoming events to a Splunk HEC | `to_splunk "localhost:8088", …`

#### Bytes

Operator | Description | Example
:--------|:------------|:-------
[`save_amqp`](./operators/save_amqp.md) | Saves incoming bytes to an AMQP server | `save_amqp`
[`save_azure_blob…`](./operators/save_azure_blob_storage.md) | Saves to an Azure Blob Storage | `save_azure_blob_storage "abfs://…`
[`save_email`](./operators/save_email.md) | Saves incoming bytes through an SMTP server | `save_email "user@example.org"`
[`save_file`](./operators/save_file.md) | Saves incoming bytes into a file | `save_file "/tmp/out.json"`
[`save_ftp`](./operators/save_ftp.md) | Saves incoming bytes via FTP | `save_ftp "ftp.example.org"`
[`save_google_cloud…`](./operators/save_google_cloud_pubsub.md) | Publishes to a Google Cloud Pub/Sub topic | `save_google_cloud_pubsub project…`
[`save_http`](./operators/save_http.md) | Sends incoming bytes over a HTTP connection | `save_http "example.org/api"`
[`save_kafka`](./operators/save_kafka.md) | Saves incoming bytes to an Apache Kafka topic | `save_kafka topic="example"`
[`save_s3`](./operators/save_s3.md) | Saves incoming bytes to an Amazon S3 object | `save_s3 "s3://my-bucket/obj.csv"`
[`save_stdout`](./operators/save_stdout.md) | Saves incoming bytes to standard output | `save_stdout`
[`save_sqs`](./operators/save_sqs.md) | Saves incoming bytes to an Amazon SQS queue | `save_sqs "sqs://tenzir"`
[`save_tcp`](./operators/save_tcp.md) | Saves incoming bytes to a TCP or TLS connection | `save_tcp "0.0.0.0:8090", tls=true`
[`save_udp`](./operators/save_udp.md) | Saves incoming bytes to a UDP socket | `save_udp "0.0.0.0:8090"`
[`save_zmq`](./operators/save_zmq.md) | Saves incoming bytes to ZeroMQ messages | `save_zmq`

## Parsing

Operator | Description | Example
:--------|:------------|:-------
[`read_bitz`](./operators/read_bitz.md) | Parses Tenzir's internal wire format | `read_bitz`
[`read_cef`](./operators/read_cef.mdx) | Parses the Common Event Format | `read_cef`
[`read_csv`](./operators/read_csv.mdx) | Parses comma-separated values | `read_csv null_value="-"`
[`read_feather`](./operators/read_feather.md) | Parses Feather format | `read_feather`
[`read_gelf`](./operators/read_gelf.mdx) | Parses the Graylog Extended Log Format | `read_gelf`
[`read_grok`](./operators/read_grok.mdx) | Parses events using a Grok pattern | `read_grok "%{IP:client} %{WORD:action}"`
[`read_json`](./operators/read_json.mdx) | Parses JSON objects | `read_json arrays_of_objects=true`
[`read_kv`](./operators/read_kv.mdx) | Parses key-value pairs | `read_kv r"(\s+)[A-Z_]+:", r":\s*"`
[`read_leef`](./operators/read_leef.mdx) | Parses the Log Event Extended Format | `read_leef`
[`read_lines`](./operators/read_lines.md) | Parses each line into a separate event | `read_lines`
[`read_ndjson`](./operators/read_ndjson.mdx) | Parses newline-delimited JSON | `read_ndjson`
[`read_pcap`](./operators/read_pcap.md) | Parses raw network packets in PCAP format | `read_pcap`
[`read_parquet`](./operators/read_parquet.md) | Parses Parquet format | `read_parquet`
[`read_ssv`](./operators/read_ssv.mdx) | Parses space-separated values | `read_ssv header="name count"`
[`read_suricata`](./operators/read_suricata.mdx) | Parses Suricata's Eve format | `read_suricata`
[`read_syslog`](./operators/read_syslog.mdx) | Parses syslog | `read_syslog`
[`read_tsv`](./operators/read_tsv.mdx) | Parses tab-separated values | `read_tsv auto_expand=true`
[`read_xsv`](./operators/read_xsv.mdx) | Parses custom-separated values | `read_xsv ";", ":", "N/A"`
[`read_yaml`](./operators/read_yaml.mdx) | Parses YAML | `read_yaml`
[`read_zeek_json`](./operators/read_zeek_json.mdx) | Parses Zeek JSON | `read_zeek_json`
[`read_zeek_tsv`](./operators/read_zeek_tsv.md) | Parses Zeek TSV | `read_zeek_tsv`

## Printing

Operator | Description | Example
:--------|:------------|:-------
[`write_bitz`](./operators/write_bitz.md) | Writes events as Tenzir's internal wire format | `write_bitz`
[`write_csv`](./operators/write_csv.md) | Writes events as CSV | `write_csv`
[`write_feather`](./operators/write_feather.md) | Writes events as Feather | `write_feather`
[`write_json`](./operators/write_json.md) | Writes events as JSON | `write_json`
[`write_kv`](./operators/write_kv.md) | Writes events as Key-Value pairs | `write_kv`
[`write_ndjson`](./operators/write_ndjson.md) | Writes events as Newline-Delimited JSON | `write_ndjson`
[`write_lines`](./operators/write_lines.md) | Writes events as lines | `write_lines`
[`write_parquet`](./operators/write_parquet.md) | Writes events as Parquet | `write_parquet`
[`write_pcap`](./operators/write_pcap.md) | Writes events as PCAP | `write_pcap`
[`write_ssv`](./operators/write_ssv.md) | Writes events as SSV | `write_ssv`
[`write_tsv`](./operators/write_tsv.md) | Writes events as TSV | `write_tsv`
[`write_tql`](./operators/write_tql.md) | Writes events as TQL objects | `write_tql`
[`write_xsv`](./operators/write_xsv.md) | Writes events as XSV | `write_xsv`
[`write_yaml`](./operators/write_yaml.md) | Writes events as YAML | `write_yaml`
[`write_zeek_tsv`](./operators/write_zeek_tsv.md) | Writes events as Zeek TSV | `write_zeek_tsv`

## Charts

Operator | Description | Example
:--------|:------------|:-------
[`chart_area`](./operators/chart_area.md) | Visualizes events on an area chart | `chart_area …`
[`chart_bar`](./operators/chart_bar.md) | Visualizes events on a bar chart | `chart_bar …`
[`chart_line`](./operators/chart_line.md) | Visualizes events on a line chart | `chart_line …`
[`chart_pie`](./operators/chart_pie.md) | Visualizes events on a pie chart | `chart_pie …`

## Connecting Pipelines

Operator | Description | Example
:--------|:------------|:-------
[`publish`](./operators/publish.md) | Publishes events to a certain topic | `publish "topic"`
[`subscribe`](./operators/subscribe.md) | Subscribes to events of a certain topic | `subscribe "topic"`

## Node

### Inspection

Operator | Description | Example
:--------|:------------|:-------
[`config`](./operators/config.md) | Returns the node's configuration | `config`
[`diagnostics`](./operators/diagnostics.md) | Returns diagnostic events of managed pipelines | `diagnostics`
[`openapi`](./operators/openapi.md) | Returns the OpenAPI specification | `openapi`
[`metrics`](./operators/metrics.md) | Retrieves metrics events from a Tenzir node | `metrics "cpu"`
[`plugins`](./operators/plugins.md) | Lists available plugins | `plugins`
[`version`](./operators/version.md) | Shows the current version | `version`

### Storage Engine

Operator | Description | Example
:--------|:------------|:-------
[`export`](./operators/export.md) | Retrieves events from the node | `export`
[`fields`](./operators/fields.md) | Lists all fields stored at the node | `fields`
[`import`](./operators/import.md) | Stores events at the node | `import`
[`partitions`](./operators/partitions.md) | Retrieves metadata about events stored at the node | `partitions src_ip == 1.2.3.4`
[`schemas`](./operators/schemas.md) | Lists schemas for events stored at the node | `schemas`

## Host Inspection

Operator | Description | Example
:--------|:------------|:-------
[`files`](./operators/files.md) | Lists files in a directory | `files "/var/log/", recurse=true`
[`nics`](./operators/nics.md) | Lists available network interfaces | `nics`
[`processes`](./operators/processes.md) | Lists running processes | `processes`
[`sockets`](./operators/sockets.md) | Lists open sockets | `sockets`

## Detection

Operator | Description | Example
:--------|:------------|:-------
[`sigma`](./operators/sigma.md) | Matches incoming events against Sigma rules | `sigma "/tmp/rules/"`
[`yara`](./operators/yara.md) | Matches the incoming byte stream against YARA rules | `yara "/path/to/rules", blockwise=true`

## Internals

Operator | Description | Example
:--------|:------------|:-------
[`api`](./operators/api.md) | Calls Tenzir's REST API from a pipeline | `api "/pipeline/list"`
[`batch`](./operators/batch.md) | Controls the batch size of events | `batch timeout=1s`
[`buffer`](./operators/buffer.md) | Adds additional buffering to handle spikes | `buffer 10M, policy="drop"`
[`cache`](./operators/cache.md) | In-memory cache shared between pipelines | `cache "w01wyhTZm3", ttl=10min`
[`legacy`](./operators/legacy.md) | Provides a compatibility fallback to TQL1 pipelines | `legacy "chart area"`
[`local`](./operators/local.md) | Forces a pipeline to run locally | `local { sort foo }`
[`measure`](./operators/measure.md) | Returns events describing the incoming batches | `measure`
[`remote`](./operators/remote.md) | Forces a pipeline to run remotely at a node | `remote { version }`
[`serve`](./operators/serve.md) | Makes events available at `/serve` | `serve "abcde12345"`
[`unordered`](./operators/unordered.md) | Remove ordering assumptions in a pipeline | `unordered { read_ndjson }`

## Encode & Decode

Operator | Description | Example
:--------|:------------|:-------
[`compress_brotli`](./operators/compress_brotli.md) | Compresses bytes using Brotli compression | `compress_zstd, level=10`
[`compress_bz2`](./operators/compress_bz2.md) | Compresses bytes using Bzip compression | `compress_bz2, level=9`
[`compress_gzip`](./operators/compress_gzip.md) | Compresses bytes using Gzip compression | `compress_gzip, level=8`
[`compress_lz4`](./operators/compress_lz4.md) | Compresses bytes using lz4 compression | `compress_lz4, level=7`
[`compress_zstd`](./operators/compress_zstd.md) | Compresses bytes using Gzip compression | `compress_zstd, level=6`
[`decompress_brotli`](./operators/decompress_brotli.md) | Decompresses Brotli compressed bytes | `decompress_zstd`
[`decompress_bz2`](./operators/decompress_bz2.md) | Decompresses Bzip2 compressed bytes | `decompress_bz2`
[`decompress_gzip`](./operators/decompress_gzip.md) | Decompresses Gzip compressed bytes | `decompress_gzip`
[`decompress_lz4`](./operators/decompress_lz4.md) | Decompresses lz4 compressed bytes | `decompress_lz4`
[`decompress_zstd`](./operators/decompress_zstd.md) | Decompresses Zstd compressed bytes | `decompress_zstd`

## Pipelines

Operator | Description | Example
:--------|:------------|:-------
[`pipeline::list`](./operators/pipeline/list.md) | Shows managed pipelines | `package::list`

## Contexts

Function | Description | Example
:--------|:------------|:-------
[`context::create_bloom_filter`](./operators/context/create_bloom_filter.md) | Creates a Bloom filter context | `context::create_bloom_filter "ctx", capacity=1Mi, fp_probability=0.01`
[`context::create_lookup_table`](./operators/context/create_lookup_table.md) | Creates a lookup table context | `context::create_lookup_table "ctx"`
[`context::create_geoip`](./operators/context/create_geoip.md) | Creates a GeoIP context for IP-based geolocation | `context::create_geoip "ctx", db_path="GeoLite2-City.mmdb"`
[`context::enrich`](./operators/context/enrich.md) | Enriches with a context | `context::enrich "ctx", key=x`
[`context::erase`](./operators/context/erase.md) | Removes entries from a context | `context::erase "ctx", key=x`
[`context::inspect`](./operators/context/inspect.md) | Inspects the details of a specified context | `context::inspect "ctx"`
[`context::list`](./operators/context/list.md) | Lists all contexts | `context::list`
[`context::remove`](./operators/context/remove.md) | Deletes a context | `context::remove "ctx"`
[`context::reset`](./operators/context/reset.md) | Resets the state of a specified context | `context::reset "ctx"`
[`context::save`](./operators/context/save.md) | Saves context state | `context::save "ctx"`
[`context::load`](./operators/context/load.md) | Loads context state | `context::load "ctx"`
[`context::update`](./operators/context/update.md) | Updates an existing context with new data | `context::update "ctx", key=x, value=y`

<!--
TBD: new name
[`context::lookup`]() | |
-->

## Packages

Operator | Description | Example
:--------|:------------|:-------
[`package::add`](./operators/package/add.md) | Installs a package | `package::add "suricata-ocsf"`
[`package::list`](./operators/package/list.md) | Shows installed packages | `package::list`
[`package::remove`](./operators/package/add.md) | Uninstalls a package | `package::remove "suricata-ocsf"`

## Escape Hatches

Operator | Description | Example
:--------|:------------|:-------
[`python`](./operators/python.md) | Executes a Python snippet for each event | `python "self.x = self.y"`
[`shell`](./operators/shell.md) | Runs a shell command within the pipeline | <code>shell "./process.sh \| tee copy.txt"</code>
