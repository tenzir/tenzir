# Introspect

With *introspection* we mean the ability of to inspect the current system state.
This concerns both health and status metrics of VAST, as well as higher-level
metadata, such as schemas and
[taxonomies](/docs/understand/data-model/taxonomies) (concepts and models).

## System Status

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

Both variations fill in more output.
