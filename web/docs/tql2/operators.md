# Operators

import './operators.css';

Tenzir comes with a wide range of built-in operators.

## Shape

Operator | Description | Example
---------|-------------|--------
[`select`](./operators/select.md) | Selects some values and discards the rest | `select name, id=metadata.id`
[`drop`](./operators/drop.md) | Removes columns from the event | `drop name, metadata.id`
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
[`where`](./operators/where.md) | Keeps only events matching a predicate | `where name.starts_with("John")`
[`assert`](./operators/assert.md) | Same as `where`, but warns if predicate is `false` | `assert name.starts_with("John")`
[`taste`](./operators/taste.md) | Keep only N events of each type | `taste 1`
[`head`](./operators/head.md) | Keep only the first N events | `head 20`
[`tail`](./operators/tail.md) | Keep only the last N events | `tail 20`
[`slice`](./operators/slice.md) | Keep a range of events with an optional stride | `slice begin=10, end=30`

<!--
[`deduplicate`]() | ... | `...`
-->

## Analytics

Operator | Description | Example
---------|-------------|--------
[`summarize`](./operators/summarize.md) | Aggregates events with implicit grouping | `summarize name, sum(transaction)`
[`sort`](./operators/sort.md) | Sorts the events by one or more expressions | `sort name, -abs(transaction)`
[`reverse`](./operators/reverse.md) | Reverses the event order | `reverse`

<!--
[`top`](./operators/top.md) | ... | `...`
[`rare`](./operators/rare.md) | ... | `...`
-->

## Flow Control

Operator | Description | Example
---------|-------------|--------
[`every`](./operators/every.md) | Restarts a pipeline periodically | `every 10s { summarize sum(transaction) }`
[`fork`](./operators/fork.md) | Forwards a copy of the events to another pipeline | `fork { to "copy.json" }`
[`if`](language/statements.md#if) | Splits the flow based on a predicate | `if transaction > 0 { ... } else { ... }`

<!--
[`group`]() | Starts a new pipeline for each group | `group path { to $path }`
[`timeout`]() | Ends a pipeline after a period without input | `timeout 1s { ... }`
[`match`]() | Splits the flow with pattern matching | `match name { "Tenzir" => {...}, _ => {...} }`
-->

## Input

Operator | Description | Example
---------|-------------|--------
[`load`](./operators/load.md) | Load bytes according to a URL | `load "https://example.org/api/list"`
[`load_file`](./operators/load_file.md) | Load bytes from a file | `load_file "/tmp/data.json"`
[`load_http`](./operators/load_http.md) | Receive bytes from a HTTP request | `load_http "example.org", params={n: 5}` |
[`load_tcp`](./operators/load_tcp.md) | Loads bytes from a TCP or TLS connection | `load_tcp "0.0.0.0:8090" { read_json }`

<!--
[`from`](./operators/from.md) | | `from "/tmp/data.json"`
-->

## Parsing

Operator | Description | Example
---------|-------------|--------
[`read_bitz`](./operators/read_bitz.md) | |
[`read_cef`](./operators/read_cef.md) | |
[`read_csv`](./operators/read_csv.md) | |
[`read_gelf`](./operators/read_gelf.md) | |
[`read_json`](./operators/read_json.md) | |
[`read_kv`](./operators/read_kv.md) | |
[`read_leef`](./operators/read_leef.md) | |
[`read_lines`](./operators/read_lines.md) | |
[`read_ndjson`](./operators/read_ndjson.md) | |
[`read_ssv`](./operators/read_ssv.md) | |
[`read_suricata`](./operators/read_suricata.md) | |
[`read_syslog`](./operators/read_syslog.md) | |
[`read_tsv`](./operators/read_tsv.md) | |
[`read_xsv`](./operators/read_xsv.md) | |
[`read_yaml`](./operators/read_yaml.md) | |
[`read_zeek_json`](./operators/read_zeek_json.md) | |
[`read_zeek_tsv`](./operators/read_zeek_tsv.md) | |

## Printing

Operator | Description | Example
---------|-------------|--------
[`write_bitz`](./operators/write_bitz.md) | |
[`write_json`](./operators/write_json.md) | |

## Output

Operator | Description | Example
---------|-------------|--------
[`discard`](./operators/discard.md) | Discard incoming bytes or events | `discard`
[`save`](./operators/save.md) | Save incoming bytes according to a URL | `save "https://example.org/api"`
[`save_file`](./operators/save_file.md) | Save incoming bytes into a file | `save_file "/tmp/out.json"`
[`save_http`](./operators/save_http.md) | Send incoming bytes over a HTTP connection | `save_http "example.org/api"`
[`serve`](./operators/serve.md) | Makes events available at the `/serve` endpoint | `serve "abcde12345"`
[`to_hive`](./operators/to_hive.md) | Writes events using hive partitioning | `to_hive "s3://my-bucket", partition_by=[field]`

## TODO: NAME?

Operator | Description | Example
---------|-------------|--------
[`diagnostics`](./operators/diagnostics.md) | |
[`export`](./operators/export.md) | |
[`import`](./operators/import.md) | |
[`metrics`](./operators/metrics.md) | |
[`publish`](./operators/publish.md) | |
[`subscribe`](./operators/subscribe.md) | |

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

## Integrations (TODO: Better name?)

Operator | Description | Example
---------|-------------|--------
[`azure_log_analytics`](./operators/azure_log_analytics.md) | |
[`sigma`](./operators/sigma.md) | |
[`velociraptor`](./operators/velociraptor.md) | |
[`yara`](./operators/yara.md) | |

## Escape Hatches (TODO: Better name?)

Operator | Description | Example
---------|-------------|--------
[`legacy`](./operators/legacy.md) | |
[`python`](./operators/python.md) | |
[`shell`](./operators/shell.md) | TODO: This is also a source... |

## Time

Operator | Description | Example
---------|-------------|--------
[`delay`](./operators/delay.md) | |
[`timeshift`](./operators/timeshift.md) | |

## Internals

Operator | Description | Example
---------|-------------|--------
[`api`](./operators/api.md) | |
[`batch`](./operators/batch.md) | |
[`buffer`](./operators/buffer.md) | |
[`measure`](./operators/measure.md) | |
[`throttle`](./operators/throttle.md) | |
[`cache`](./operators/cache.md) | In-memory cache shared between pipelines | `cache "w01wyhTZm3", ttl=10min`

## Introspection

Operator | Description | Example
---------|-------------|--------
[`config`](./operators/config.md) | |
[`fields`](./operators/fields.md) | |
[`openapi`](./operators/openapi.md) | |
[`partitions`](./operators/partitions.md) | |
[`plugins`](./operators/plugins.md) | |
[`schemas`](./operators/schemas.md) | |
[`version`](./operators/version.md) | |

## Host System

Operator | Description | Example
---------|-------------|--------
[`files`](./operators/files.md) | |
[`nics`](./operators/nics.md) | |
[`processes`](./operators/processes.md) | |
[`sockets`](./operators/sockets.md) | |

## UNCATEGORIZED
Operator | Description | Example
---------|-------------|--------
[`compress`](./operators/compress.md) | |
[`decompress`](./operators/decompress.md) | |
[`pass`](./operators/pass.md) | |
[`repeat`](./operators/repeat.md) | |
