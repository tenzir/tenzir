# Operators

import './operators.css';

Tenzir comes with a wide range of built-in operators.

## Shape

Operator | Description | Example
---------|-------------|--------
[`select`](./operators/select.md) | Select some values and discard the rest | `select name, id=metadata.id`
[`drop`](./operators/drop.md) | Remove columns from the event | `drop name, metadata.id`
[`enumerate`](./operators/enumerate.md) | Add a field with the number of the event | `enumerate num`

<!--
TODO: Do we want to document set, and if so, how?
[`set`]() | Explicitly assigns one or multiple values | `set name="Tenzir", id=metadata.id`
[`unroll`]() | ... | `...`
[`yield`]() | ... | `...`
-->

## Filter

Operator | Description | Example
---------|-------------|--------
[`where`](./operators/where.md) | Keep only events matching a predicate | `where name.starts_with("John")`
[`assert`](./operators/assert.md) | Same as `where`, but warns if predicate is `false` | `assert name.starts_with("John")`
[`taste`](./operators/taste.md) | Keep only N events of each type | `taste 1`
[`head`](./operators/head.md) | Keep only the first N events | `head 20`
[`tail`](./operators/tail.md) | Keep only the last N events | `tail 20`
[`slice`](./operators/slice.md) | Keep a range of events with an optional stride | `slice begin=10, end=30`

<!--
[`deduplicate`]() | ... | `...`
-->

## Analyze

Operator | Description | Example
---------|-------------|--------
[`summarize`](./operators/summarize.md) | Aggregate events with implicit grouping | `summarize name, sum(transaction)`
[`sort`](./operators/sort.md) | Sort the events by one or more expressions | `sort name, -abs(transaction)`
[`reverse`](./operators/reverse.md) | Reverse the event order | `reverse`

<!--
[`top`](./operators/top.md) | ... | `...`
[`rare`](./operators/rare.md) | ... | `...`
-->

## Flow Control

Operator | Description | Example
---------|-------------|--------
[`every`](./operators/every.md) | Restart a pipeline periodically | `every 10s { summarize sum(transaction) }`
[`fork`](./operators/fork.md) | Forward a copy of the events to another pipeline | `fork { to "copy.json" }`
[`if`](language/statements.md#if) | Split the flow based on a predicate | `if transaction > 0 { ... } else { ... }`

<!--
[`group`]() | Starts a new pipeline for each group | `group path { to $path }`
[`timeout`]() | Ends a pipeline after a period without input | `timeout 1s { ... }`
[`match`]() | Splits the flow with pattern matching | `match name { "Tenzir" => {...}, _ => {...} }`
-->

## Input

Operator | Description | Example
---------|-------------|--------
[`diagnostics`](./operators/diagnostics.md) | Retrieve diagnostic events of managed pipelines | `diagnostics`
[`export`](./operators/export.md) | Retrieve events from the node | `export`
[`load`](./operators/load.md) | Load bytes according to a URL | `load "https://example.org/api/list"`
[`load_file`](./operators/load_file.md) | Load bytes from a file | `load_file "/tmp/data.json"`
[`load_http`](./operators/load_http.md) | Receive bytes from a HTTP request | `load_http "example.org", params={n: 5}`
[`load_tcp`](./operators/load_tcp.md) | Load bytes from a TCP or TLS connection | `load_tcp "0.0.0.0:8090" { read_json }`
[`velociraptor`](./operators/velociraptor.md) | Returns results from a Velociraptor server | `velociraptor subscribe="Windows"`
[`metrics`](./operators/metrics.md) | Retrieve metrics events from a Tenzir node | `metrics "cpu"`
[`subscribe`](./operators/subscribe.md) | Subscribe to a certain topic | `subscribe "topic"`
[`shell`](./operators/shell.md) | TODO: This is also a transformation |
<!--
[`from`](./operators/from.md) | | `from "/tmp/data.json"`
-->

## Output

Operator | Description | Example
---------|-------------|--------
[`azure_log_analytics`](./operators/azure_log_analytics.md) | Send events to Azure Log Analytics | `azure_log_analytics tenant_id=…`
[`discard`](./operators/discard.md) | Discard incoming bytes or events | `discard`
[`save_file`](./operators/save_file.md) | Save incoming bytes into a file | `save_file "/tmp/out.json"`
[`save_http`](./operators/save_http.md) | Send incoming bytes over a HTTP connection | `save_http "example.org/api"`
[`save`](./operators/save.md) | Save incoming bytes according to a URL | `save "https://example.org/api"`
[`serve`](./operators/serve.md) | Makes events available at `/serve` | `serve "abcde12345"`
[`to_hive`](./operators/to_hive.md) | Writes events using hive partitioning | `to_hive "s3://…", partition_by=[x]`
[`import`](./operators/import.md) | Store events at the node | `import`
[`publish`](./operators/publish.md) | Publish events to a certain topic | `publish "topic"`


## Parsing

Operator | Description | Example
---------|-------------|--------
[`read_bitz`](./operators/read_bitz.md) | Parses input as Tenzir's internal wire format | `read_bitz`
[`read_cef`](./operators/read_cef.md) | Parses input as [CEF](https://community.microfocus.com/cfs-file/__key/communityserver-wikis-components-files/00-00-00-00-23/3731.CommonEventFormatV25.pdf) messages | `read_cef`
[`read_csv`](./operators/read_csv.md) | Parses input as [comma separated values](https://en.wikipedia.org/wiki/Comma-separated_values) | `read_csv`
[`read_gelf`](./operators/read_gelf.md) | Parses input as [GELF](https://go2docs.graylog.org/current/getting_in_log_data/gelf.html) | `read_gelf`
[`read_grok`](./operators/read_grok.md) | Parses input into events using a grok pattern | `read_grok`
[`read_json`](./operators/read_json.md) | Parses input as [JSON](https://en.wikipedia.org/wiki/JSON) | `read_jsom`
[`read_kv`](./operators/read_kv.md) | Parses input as key-value pairs | `read_kv`
[`read_leef`](./operators/read_leef.md) | Parses input as [leef](https://www.ibm.com/docs/en/dsm?topic=overview-leef-event-components) | `read_leef`
[`read_lines`](./operators/read_lines.md) | Parses input as entire lines | `read_lines`
[`read_ndjson`](./operators/read_ndjson.md) | Reads input as newline delimited [JSON](https://en.wikipedia.org/wiki/JSON) | `read_leef`
[`read_ssv`](./operators/read_ssv.md) | Parses input as space separated values | `read_csv`
[`read_suricata`](./operators/read_suricata.md) | Parses input as [suricata eve.json](https://suricata.readthedocs.io/en/latest/output/eve/eve-json-output.html) | `read_suricata`
[`read_syslog`](./operators/read_syslog.md) | Parses input as [Syslog](https://en.wikipedia.org/wiki/Syslog) | `read_syslog`
[`read_tsv`](./operators/read_tsv.md) | Parses input as tab separated values | `read_tsv`
[`read_xsv`](./operators/read_xsv.md) | Parses input as "generic" separated values | `read_xsv "," ";" ""`
[`read_yaml`](./operators/read_yaml.md) | Parses input as [YAML](https://en.wikipedia.org/wiki/YAML) | `read_yaml`
[`read_zeek_json`](./operators/read_zeek_json.md) | Parses input as Zeek JSON | `read_zeek_json`
[`read_zeek_tsv`](./operators/read_zeek_tsv.md) | Parses input as Zeek TSV | `read_zeek_tsv`

## Printing

Operator | Description | Example
---------|-------------|--------
[`write_bitz`](./operators/write_bitz.md) | Writes events as Tenzir's internal wire format | `write_bitz`
[`write_json`](./operators/write_json.md) | Writes events as JSON | `write_json`

<!--
## Packages

Operator | Description | Example
---------|-------------|--------
[`package::add`]() | |
[`package::remove`]() | |
[`package::list`]() | |

## Contexts

Operator | Description | Example
---------|-------------|--------
[`context::create`]() | |
[`context::delete`]() | |
[`context::update`]() | |
[`context::lookup`]() | |

## Charts

Operator | Description | Example
---------|-------------|--------
[`bar_chart`]() | All of this is TBD. |
[`treemap`]() | |
[`line_chart`]() | |
-->

## Internals

Operator | Description | Example
---------|-------------|--------
[`api`](./operators/api.md) | |
[`batch`](./operators/batch.md) | |
[`buffer`](./operators/buffer.md) | |
[`measure`](./operators/measure.md) | |
[`throttle`](./operators/throttle.md) | |
[`cache`](./operators/cache.md) | In-memory cache shared between pipelines | `cache "w01wyhTZm3", ttl=10min`
[`legacy`](./operators/legacy.md) | |

## Node Inspection

Operator | Description | Example
---------|-------------|--------
[`config`](./operators/config.md) | Node's configuration | `config`
[`fields`](./operators/fields.md) | List all fields stored at the node | `fields`
[`openapi`](./operators/openapi.md) | Node's OpenAPI specification | `openapi`
[`partitions`](./operators/partitions.md) | Retrieve metadata about events stored at the node | `partitions src_ip == 1.2.3.4`
[`plugins`](./operators/plugins.md) | List available plugins | `plugins`
[`schemas`](./operators/schemas.md) | List schemas for events stored at the node | `schemas`
[`version`](./operators/version.md) | Version Info | `version`

## Host Inspection

Operator | Description | Example
---------|-------------|--------
[`files`](./operators/files.md) | List files in a directory | `files "/var/log/" recurse=true`
[`nics`](./operators/nics.md) | List available network interfaces | `nics`
[`processes`](./operators/processes.md) | List running processes | `processes`
[`sockets`](./operators/sockets.md) | List open sockets | `sockets`

## Uncategorized

Operator | Description | Example
---------|-------------|--------
[`compress`](./operators/compress.md) | |
[`decompress`](./operators/decompress.md) | |
[`delay`](./operators/delay.md) | |
[`pass`](./operators/pass.md) | |
[`repeat`](./operators/repeat.md) | |
[`sigma`](./operators/sigma.md) | |
[`timeshift`](./operators/timeshift.md) | |
[`yara`](./operators/yara.md) | |
[`python`](./operators/python.md) |  | `python "self.x ="`
