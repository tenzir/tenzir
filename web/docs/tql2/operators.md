# Operators

import './operators.css';

Tenzir comes with a wide range of built-in operators.

## Shape

Operator | Description | Example
---------|-------------|--------
[`select`](operators/select) | Selects values and discards the rest | `select name, id=metadata.id`
[`drop`](operators/drop) | Removes columns from the event | `drop name, metadata.id`
[`enumerate`](operators/enumerate) | Add a field with the number of the event | `enumerate num`

<!--
TODO: Do we want to document set, and if so, how?
[`set`](operators/set) | Explicitly assigns one or multiple values | `set name="Tenzir", id=metadata.id`
[`unroll`](operators/unroll) | ... | `...`
[`yield`](operators/yield) | ... | `...`
-->

## Filter

Operator | Description | Example
---------|-------------|--------
[`where`](operators/where) | Keeps only events matching a predicate | `where name.starts_with("John")`
[`assert`](operators/assert) | Same as `where`, but warns if predicate is `false` | `assert name.starts_with("John")`
[`taste`](operators/taste) | Keep only N events of each type | `taste 1`
[`head`](operators/head) | Keep only the first N events | `head 20`
[`tail`](operators/tail) | Keep only the last N events | `tail 20`
[`slice`](operators/slice) | Keep a range of events with an optional stride | `slice begin=10, end=30`

<!--
[`deduplicate`](operators/deduplicate) | ... | `...`
-->

## Analytics

Operator | Description | Example
---------|-------------|--------
[`summarize`](operators/summarize) | Aggregates events with implicit grouping | `summarize name, sum(transaction)`
[`sort`](operators/sort) | Sorts the events by one or more expressions | `sort name, -abs(transaction)`
[`reverse`](operators/reverse) | ... | `...`

<!--
[`top`](operators/top) | ... | `...`
[`rare`](operators/rare) | ... | `...`
-->

## Flow Control

Operator | Description | Example
---------|-------------|--------
[`every`](operators/every) | Restarts a pipeline periodically | `every 10s { summarize sum(transaction) }`
[`fork`](operators/fork) | Forwards a copy of the events to another pipeline | `fork { to "copy.json" }`
[`if`](language/statements.md#if) | Splits the flow based on a predicate | `if transaction > 0 { ... } else { ... }`

<!--
[`group`](operators/group) | Starts a new pipeline for each group | `group path { to $path }`
[`timeout`](operators/timeout) | Ends a pipeline after a period without input | `timeout 1s { ... }`
[`match`](operators/match) | Splits the flow with pattern matching | `match name { "Tenzir" => {...}, _ => {...} }`
-->

## Input

Operator | Description | Example
---------|-------------|--------
[`from`](operators/from) | | `from "file:///tmp/data.json"`
[`load`](operators/load) | | `load "file:///tmp/data.json"`
[`load_file`](operators/load_file) | | `load_file "/tmp/data.json"`
[`load_http`](operators/load_http) |  | `load_http "example.org", params={n: 5}` |
[`load_tcp`](operators/load_tcp) | Loads bytes from a TCP or TLS connection | `load_tcp "0.0.0.0:8090" { read_json }`

## Parsing

Operator | Description | Example
---------|-------------|--------
[`read_bitz`](operators/read_bitz) | |
[`read_cef`](operators/read_cef) | |
[`read_csv`](operators/read_csv) | |
[`read_gelf`](operators/read_gelf) | |
[`read_json`](operators/read_json) | |
[`read_kv`](operators/read_kv) | |
[`read_leef`](operators/read_leef) | |
[`read_lines`](operators/read_lines) | |
[`read_ndjson`](operators/read_ndjson) | |
[`read_ssv`](operators/read_ssv) | |
[`read_suricata`](operators/read_suricata) | |
[`read_syslog`](operators/read_syslog) | |
[`read_tsv`](operators/read_tsv) | |
[`read_xsv`](operators/read_xsv) | |
[`read_yaml`](operators/read_yaml) | |
[`read_zeek_json`](operators/read_zeek_json) | |
[`read_zeek_tsv`](operators/read_zeek_tsv) | |

## Printing

Operator | Description | Example
---------|-------------|--------
[`write_bitz`](operators/write_bitz) | |
[`write_json`](operators/write_json) | |

## Output

Operator | Description | Example
---------|-------------|--------
[`discard`](operators/discard) | Not really though. |
[`save`](operators/save) | |
[`save_file`](operators/save_file) | |
[`save_http`](operators/save_http) | |
[`serve`](operators/serve) | |
[`to_hive`](operators/to_hive) | |

## TODO: NAME?

Operator | Description | Example
---------|-------------|--------
[`diagnostics`](operators/) | |
[`export`](operators/) | |
[`import`](operators/) | |
[`metrics`](operators/) | |
[`publish`](operators/) | |
[`subscribe`](operators/) | |

<!--
## Packages

Operator | Description | Example
---------|-------------|--------
[`package::add`](operators/) | |
[`package::remove`](operators/) | |
[`package::list`](operators/) | |

## Contexts

Operator | Description | Example
---------|-------------|--------
[`context::create`](operators/) | |
[`context::delete`](operators/) | |
[`context::update`](operators/) | |
[`context::lookup`](operators/) | |

## Charts

Operator | Description | Example
---------|-------------|--------
[`bar_chart`](operators/) | All of this is TBD. |
[`treemap`](operators/) | |
[`line_chart`](operators/) | |
-->

## Integrations (TODO: Better name?)

Operator | Description | Example
---------|-------------|--------
[`azure_log_analytics`](operators/) | |
[`sigma`](operators/) | |
[`velociraptor`](operators/) | |
[`yara`](operators/) | |

## Escape Hatches (TODO: Better name?)

Operator | Description | Example
---------|-------------|--------
[`legacy`](operators/) | |
[`python`](operators/) | |
[`shell`](operators/) | TODO: This is also a source... |

## Time

Operator | Description | Example
---------|-------------|--------
[`delay`](operators/) | |
[`timeshift`](operators/) | |

## Internals

Operator | Description | Example
---------|-------------|--------
[`api`](operators/api) | |
[`batch`](operators/batch) | |
[`buffer`](operators/buffer) | |
[`measure`](operators/measure) | |
[`throttle`](operators/throttle) | |
[`cache`](operators/cache) | In-memory cache shared between pipelines | `cache "w01wyhTZm3", ttl=10min`

## Introspection

Operator | Description | Example
---------|-------------|--------
[`config`](operators/) | |
[`fields`](operators/) | |
[`openapi`](operators/) | |
[`partitions`](operators/) | |
[`plugins`](operators/) | |
[`schemas`](operators/) | |
[`version`](operators/) | |

## Host System

Operator | Description | Example
---------|-------------|--------
[`files`](operators/) | |
[`nics`](operators/) | |
[`processes`](operators/) | |
[`sockets`](operators/) | |

## UNCATEGORIZED
Operator | Description | Example
---------|-------------|--------
[`compress`](operators/compress) | |
[`decompress`](operators/decompress) | |
[`pass`](operators/pass) | |
[`repeat`](operators/repeat) | |
