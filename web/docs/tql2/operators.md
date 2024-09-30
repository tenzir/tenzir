# Operators

import './operators.css';

## Basic

Operator | Description | Example
---------|-------------|--------
[`drop`](operators/drop) | Removes columns from the event | `drop name, metadata.id`
[`head`](operators/head) | Keep only the first N events | `head 20`
[`select`](operators/select) | Selects values and discards the rest | `select name, id=metadata.id`
[`set`](operators/set) | Explicitly assigns one or multiple values | `set name="Tenzir", id=metadata.id`
[`sort`](operators/sort) | Sorts the events by one or more expressions | `sort name, -abs(transaction)`
[`summarize`](operators/summarize) | Aggregates events with implicit grouping | `summarize name, sum(transaction)`
[`tail`](operators/tail) | Keep only the last N events | `tail 20`
[`taste`](operators/taste) | Keep only N events of each type | `taste 1`
[`where`](operators/where) | Keeps only events matching the predicate| `where name.starts_with("John")`

## Flow Control

Operator | Description | Example
---------|-------------|--------
[`every`](operators/every) | Restarts a pipeline periodically | `every 10s { summarize sum(transaction) }`
[`fork`](operators/fork) | Forwards a copy of the events to another pipeline | `fork { to "copy.json" }`
[`group`](operators/group) | Starts a new pipeline for each group | `group path { to $path }`
[`if`](operators/if) | Splits the flow based on a predicate | `if transaction > 0 { ... } else { ... }`
[`match`](operators/match) | Splits the flow with pattern matching | `match name { "Tenzir" => {...}, _ => {...} }`
[`timeout`](operators/timeout) | Ends a pipeline after a period without input | `timeout 1s { ... }`

## Input

Operator | Description | Example
---------|-------------|--------
[`from`](operators/from) | |
[`load`](operators/load) | |
[`load_file`](operators/load_file) | |
[`load_http`](operators/load_http) | |


Operator | Description | Example
---------|-------------|--------
**Fundamental** | *Basic operations expected everywhere*
[`drop`](operators/drop) | Removes columns from the event | `drop name, metadata.id`
[`head`](operators/head) | Keep only the first N events | `head 20`
[`select`](operators/select) | Selects values and discards the rest | `select name, id=metadata.id`
[`set`](operators/set) | Explicitly assigns one or multiple values | `set name="Tenzir", id=metadata.id`
[`sort`](operators/sort) | Sorts the events by one or more expressions | `sort name, -abs(transaction)`
[`summarize`](operators/summarize) | Aggregates events with implicit grouping | `summarize name, sum(transaction)`
[`tail`](operators/tail) | Keep only the last N events | `tail 20`
[`taste`](operators/taste) | Keep only N events of each type | `taste 1`
[`where`](operators/where) | Keeps only events matching the predicate| `where name.starts_with("John")`
**Flow Control** | *Changes the data flow through the system*
[`every`](operators/every) | Restarts a pipeline periodically | `every 10s { summarize sum(transaction) }`
[`fork`](operators/fork) | Forwards a copy of the events to another pipeline | `fork { to "copy.json" }`
[`group`](operators/group) | Starts a new pipeline for each group | `group path { to $path }`
[`if`](operators/if) | Splits the flow based on a predicate | `if transaction > 0 { ... } else { ... }`
[`match`](operators/match) | Splits the flow with pattern matching | `match name { "Tenzir" => {...}, _ => {...} }`
[`timeout`](operators/timeout) | Ends a pipeline after a period without input | `timeout 1s { ... }`
**Input** | *Returns potentially external data without input*
[`from`](operators/from) | |
[`load`](operators/load) | |
[`load_file`](operators/load_file) | |
[`load_http`](operators/load_http) | |
**Parsing** | *...*
[`read_csv`](operators/) | |
[`read_json`](operators/read_json) | |
[`read_lines`](operators/) | |
[`read_suricata`](operators/) | |
[`read_syslog`](operators/) | |
[`read_yaml`](operators/) | |
[`read_zeek_json`](operators/) | |
[`read_zeek_tsv`](operators/) | |
**Printing** | *...*
[`write_json`](operators/) | |
**Output** | *...*
[`discard`](operators/) | Not really though. |
[`save`](operators/) | |
[`save_file`](operators/) | |
[`save_http`](operators/) | |
[`serve`](operators/) | |
**Not sure** | *...*
[`diagnostics`](operators/) | |
[`export`](operators/) | |
[`import`](operators/) | |
[`metrics`](operators/) | |
[`publish`](operators/) | |
[`subscribe`](operators/) | |
**Packages** | *...*
[`package::add`](operators/) | |
[`package::remove`](operators/) | |
[`package::list`](operators/) | |
**Contexts** | *...*
[`context::create`](operators/) | |
[`context::delete`](operators/) | |
[`context::update`](operators/) | |
[`context::lookup`](operators/) | |
**Charts** | *...*
[`bar_chart`](operators/) | |
[`treemap`](operators/) | |
[`line_chart`](operators/) | |
**Integrations** | *...*
[`azure_log_analytics`](operators/) | |
[`sigma`](operators/) | |
[`velociraptor`](operators/) | |
[`yara`](operators/) | |
**Escape Hatches** | *...*
[`legacy`](operators/) | |
[`python`](operators/) | |
[`shell`](operators/) | |
**Timing** | *...*
[`delay`](operators/) | |
[`timeshift`](operators/) | |
**Internals** | *...*
[`api`](operators/) | |
[`batch`](operators/) | |
[`buffer`](operators/) | |
[`measure`](operators/) | |
[`throttle`](operators/) | |
**Introspection** | *...*
[`config`](operators/) | |
[`fields`](operators/) | |
[`openapi`](operators/) | |
[`partitions`](operators/) | |
[`plugins`](operators/) | |
[`schemas`](operators/) | |
[`version`](operators/) | |
**Host System** | *...*
[`files`](operators/) | |
[`nics`](operators/) | |
[`processes`](operators/) | |
[`sockets`](operators/) | |



#### Not sure how to call this
enumerate
rare
repeat
top
unroll

#### Not sure how to call this
assert
deduplicate
reverse
slice

#### UNCATEGORIZED
compress
decompress
pass
yield
