# Introspect

With *introspection* we mean the ability of to inspect the current system state.
This concerns both health and status metrics of VAST, as well as higher-level
metadata, such as event schemas and
[taxonomies](/docs/understand/data-model/taxonomies) (concepts and models).

## Inspect the status of a VAST node

The `status` command displays a variety of system information. Without any
arguments, it provides a high-level overview in JSON output:

```bash
vast status
```

Example output looks as follows:

```json
{
  "archive": {
    "events": 0,
    "memory-usage": 0
  },
  "catalog": {
    "memory-usage": 0,
    "num-partitions": 0
  },
  "filesystem": {
    "type": "POSIX"
  },
  "importer": {
    "transformer": {
      "pipelines": []
    }
  },
  "index": {
    "memory-usage": 0,
    "statistics": {
      "events": {
        "total": 0
      }
    }
  },
  "system": {
    "current-memory-usage": 499281920,
    "database-path": "/var/lib/vast",
    "in-memory-table-slices": 0,
    "peak-memory-usage": 499281920,
    "swap-space-usage": 0
  },
  "version": {
    "Apache Arrow": "10.0.0",
    "Build Configuration": {
      "Address Sanitizer": false,
      "Assertions": false,
      "Tree Hash": "8a718abf17a268f95ff60772b57f9294",
      "Type": "Release",
      "Undefined Behavior Sanitizer": false
    },
    "CAF": "0.17.6",
    "VAST": "v2.4.0-rc2-97-g52d30f9834",
    "plugins": {
      "broker": "v1.0.0-gfc9827be2d",
      "parquet": "v1.0.0-g027ba25701",
      "pcap": "v1.1.0-gfc9827be2d",
      "sigma": "v1.1.0-g128d0b45a3",
      "web": "v1.0.0-g21258eda7c"
    }
  }
}
```

The returned top-level JSON object has one key per
[component](/docs/understand/architecture/components), plus the two "global" keys
`system` and `version`.

There exist two variations that add more detailed output:

1. `vast status --detailed`
2. `vast status --debug`

Both variations fill in more output in the respective component sections.

## Describe event fields and types

When you want to know "what's in my VAST node?" so that you can write queries,
use the `show` command. If you're familiar with SQL databases, such as
[DuckDB](https://duckdb.org/docs/guides/meta/list_tables), the `show` equivalent
would be `SHOW TABLES` or `DESCRIBE`.

You can invoke the `show` command with three positional arguments:

1. `vast show concepts`
2. `vast show models`
3. `vast show schemas`

Options (1) and (2) show taxonomy details about concepts and models, and (3)
displays all known types, both from statically specified schemas in
configuration files as well as dynamically generated schemas at runtime.

Example output of (3) may look as follows:

```bash
vast show schemas
```

```json
[
  {
    "suricata.flow": {
      "record": {
        "timestamp": {
          "timestamp": "time"
        },
        "flow_id": {
          "type": "count",
          "attributes": {
            "index": "hash"
          }
        },
        "pcap_cnt": "count",
        "vlan": {
          "list": "count"
        },
        "in_iface": "string",
        "src_ip": "addr",
        "src_port": {
          "port": "count"
        },
        "dest_ip": "addr",
        "dest_port": {
          "port": "count"
        },
        "proto": "string",
        "event_type": "string",
        "community_id": {
          "type": "string",
          "attributes": {
            "index": "hash"
          }
        },
        "flow": {
          "suricata.component.flow": {
            "record": {
              "pkts_toserver": "count",
              "pkts_toclient": "count",
              "bytes_toserver": "count",
              "bytes_toclient": "count",
              "start": "time",
              "end": "time",
              "age": "count",
              "state": "string",
              "reason": "string",
              "alerted": "bool"
            }
          }
        },
        "app_proto": "string"
      }
    }
  },
  {
    "suricata.stats": {
      "record": {
        "timestamp": {
          "timestamp": "time"
        }
      }
    }
  }
]
```
  
You can also render the output in YAML:

```bash
vast show schemas --yaml
```

```yaml
- suricata.flow:
    record:
      timestamp:
        timestamp: time
      flow_id:
        type: count
        attributes:
          index: hash
      pcap_cnt: count
      vlan:
        list: count
      in_iface: string
      src_ip: addr
      src_port:
        port: count
      dest_ip: addr
      dest_port:
        port: count
      proto: string
      event_type: string
      community_id:
        type: string
        attributes:
          index: hash
      flow:
        suricata.component.flow:
          record:
            pkts_toserver: count
            pkts_toclient: count
            bytes_toserver: count
            bytes_toclient: count
            start: time
            end: time
            age: count
            state: string
            reason: string
            alerted: bool
      app_proto: string
- suricata.stats:
    record:
      timestamp:
        timestamp: time
```

:::note JSON Schema
Semantically, this output is similar to [JSON Schema](https://json-schema.org/).
In VAST's [type system](/docs/understand/data-model/type-system) value
constraints (e.g., minimum value, maximum string length) correspond to type
attributes, which are free-form key-value pairs. To date, VAST does not actively
support enforcing type constraints via attributes, but will rely on this
mechanism for this purpose in the future.
:::
