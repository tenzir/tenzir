# Operators

import './operators.css';

## Shaping

Operator | Description | Example
---------|-------------|--------
[`select`](operators/select) | Selects values and discards the rest | `select name, id=metadata.id`
[`drop`](operators/drop) | Removes columns from the event | `drop name, metadata.id`
[`set`](operators/set) | Explicitly assigns one or multiple values | `set name="Tenzir", id=metadata.id`
[`enumerate`](operators/enumerate) | ... | `...`
[`unroll`](operators/unroll) | ... | `...`
[`yield`](operators/yield) | ... | `...`

## Filtering

Operator | Description | Example
---------|-------------|--------
[`where`](operators/where) | Keeps only events matching the predicate| `where name.starts_with("John")`
[`assert`](operators/assert) | ... | `...`
[`deduplicate`](operators/deduplicate) | ... | `...`
[`taste`](operators/taste) | Keep only N events of each type | `taste 1`
[`head`](operators/head) | Keep only the first N events | `head 20`
[`tail`](operators/tail) | Keep only the last N events | `tail 20`
[`slice`](operators/slice) | ... | `...`

## Analytics

Operator | Description | Example
---------|-------------|--------
[`summarize`](operators/summarize) | Aggregates events with implicit grouping | `summarize name, sum(transaction)`
[`top`](operators/top) | ... | `...`
[`rare`](operators/rare) | ... | `...`
[`sort`](operators/sort) | Sorts the events by one or more expressions | `sort name, -abs(transaction)`
[`reverse`](operators/reverse) | ... | `...`

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

## Parsing

Operator | Description | Example
---------|-------------|--------
[`read_csv`](operators/) | |
[`read_json`](operators/read_json) | |
[`read_lines`](operators/) | |
[`read_suricata`](operators/) | |
[`read_syslog`](operators/) | |
[`read_yaml`](operators/) | |
[`read_zeek_json`](operators/) | |
[`read_zeek_tsv`](operators/) | |

## Printing

Operator | Description | Example
---------|-------------|--------
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
[`api`](operators/) | |
[`batch`](operators/) | |
[`buffer`](operators/) | |
[`measure`](operators/) | |
[`throttle`](operators/) | |

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
