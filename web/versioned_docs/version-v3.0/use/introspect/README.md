# Introspect

With *introspection* we mean the ability of to inspect the current system state.
This concerns both health and status metrics of VAST, as well as higher-level
metadata, such as event schemas and [taxonomies][taxonomies] (concepts and
models).

## Inspect the status of a VAST node

The `status` command displays a variety of system information. Without any
arguments, it provides a high-level overview in JSON output:

```bash
vast status
```

```json
{
  "catalog": {
    "memory-usage": 1512,
    "num-events": 2,
    "num-partitions": 2,
    "schemas": {
      "suricata.alert": {
        "import-time": {
          "max": "2023-01-11T15:14:59.921171",
          "min": "2023-01-11T15:14:59.921171"
        },
        "num-events": 1,
        "num-partitions": 1
      },
      "suricata.dns": {
        "import-time": {
          "max": "2023-01-11T15:14:59.920248",
          "min": "2023-01-11T15:14:59.920248"
        },
        "num-events": 1,
        "num-partitions": 1
      }
    }
  },
  "disk-monitor": {
    "disk-monitor": {
      "blacklist-size": 0
    }
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
    "memory-usage": 0
  },
  "system": {
    "current-memory-usage": 499281920,
    "database-path": "/var/lib/vast",
    "in-memory-table-slices": 2,
    "peak-memory-usage": 499281920
  },
  "version": {
    "Apache Arrow": "10.0.1",
    "Build Configuration": {
      "Address Sanitizer": true,
      "Assertions": true,
      "Tree Hash": "54256390cff0a8ed63218140c35b54f3",
      "Type": "Debug",
      "Undefined Behavior Sanitizer": false
    },
    "CAF": "0.18.6",
    "VAST": "v2.4.0-583-gade8a85ac4-dirty",
    "plugins": {
      "cef": "v0.1.0-g314fcdd30c",
      "parquet": "v1.0.0-g314fcdd30c",
      "pcap": "v1.1.0-g314fcdd30c",
      "sigma": "v1.1.0-g2b0cf481e4",
      "web": "v1.0.0-g0bcf9abed8"
    }
  }
}
```

The returned top-level JSON object has one key per component, plus the two
"global" keys `system` and `version`.

There exist two variations that add more detailed output:

1. `vast status --detailed`
2. `vast status --debug`

Both variations fill in more output in the respective component sections.

## Describe event schemas and taxonomies

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

### Describe event fields and types

The default output is JSON for easy post-processing. You can also pass `--yaml`
for a more human-readable structure after any of the positional arguments. For
example:

```bash
vast show schemas --yaml
```

```yaml
- suricata.flow:
    record:
      - timestamp:
          timestamp: time
      - flow_id:
          type: uint64
          attributes:
            index: hash
      - pcap_cnt: uint64
      - vlan:
          list: uint64
      - in_iface: string
      - src_ip: ip
      - src_port:
          port: uint64
      - dest_ip: ip
      - dest_port:
          port: uint64
      - proto: string
      - event_type: string
      - community_id:
          type: string
          attributes:
            index: hash
      - flow:
          suricata.component.flow:
            record:
              - pkts_toserver: uint64
              - pkts_toclient: uint64
              - bytes_toserver: uint64
              - bytes_toclient: uint64
              - start: time
              - end: time
              - age: uint64
              - state: string
              - reason: string
              - alerted: bool
      - app_proto: string
```

<details>
<summary>JSON equivalent of the above YAML output</summary>

```json
[
  {
    "suricata.flow": {
      "record": [
        {
          "timestamp": {
            "timestamp": "time"
          }
        },
        {
          "flow_id": {
            "type": "uint64",
            "attributes": {
              "index": "hash"
            }
          }
        },
        {
          "pcap_cnt": "uint64"
        },
        {
          "vlan": {
            "list": "uint64"
          }
        },
        {
          "in_iface": "string"
        },
        {
          "src_ip": "ip"
        },
        {
          "src_port": {
            "port": "uint64"
          }
        },
        {
          "dest_ip": "ip"
        },
        {
          "dest_port": {
            "port": "uint64"
          }
        },
        {
          "proto": "string"
        },
        {
          "event_type": "string"
        },
        {
          "community_id": {
            "type": "string",
            "attributes": {
              "index": "hash"
            }
          }
        },
        {
          "flow": {
            "suricata.component.flow": {
              "record": [
                {
                  "pkts_toserver": "uint64"
                },
                {
                  "pkts_toclient": "uint64"
                },
                {
                  "bytes_toserver": "uint64"
                },
                {
                  "bytes_toclient": "uint64"
                },
                {
                  "start": "time"
                },
                {
                  "end": "time"
                },
                {
                  "age": "uint64"
                },
                {
                  "state": "string"
                },
                {
                  "reason": "string"
                },
                {
                  "alerted": "bool"
                }
              ]
            }
          }
        },
        {
          "app_proto": "string"
        }
      ]
    }
  }
]
```

</details>

Semantically, `vast show schemas` is to VAST data what [JSON
Schema](https://json-schema.org/) is to JSON. In VAST's [type
system](../../understand/data-model/type-system.md) value constraints (e.g.,
minimum value, maximum string length) correspond to type attributes, which are
free-form key-value pairs. To date, VAST does not actively support enforcing
type constraints via attributes, but will rely on this mechanism for this
purpose in the future.

### Describe concepts and models

The other two arguments to `show` commands display data-independent
[taxonomy][taxonomies] configuration.

For example, you can display all concepts as follows:

```bash
vast show concepts --yaml
```

```yaml
- concept:
    name: net.app
    description: The application-layer protocol of a connection
    fields:
      - suricata.alert.alert.app_proto
      - suricata.dcerpc.event_type
      - suricata.dhcp.event_type
      - suricata.dns.event_type
      - suricata.ftp.event_type
      - suricata.ftp_data.event_type
      - suricata.http.event_type
      - suricata.fileinfo.app_proto
      - suricata.flow.app_proto
      - suricata.ikev2.app_proto
      - suricata.krb5.event_type
      - suricata.mqtt.event_type
      - suricata.netflow.app_proto
      - suricata.nfs.app_proto
      - suricata.rdp.app_proto
      - suricata.rfb.app_proto
      - suricata.sip.app_proto
      - suricata.smb.event_type
      - suricata.ssh.event_type
      - suricata.smtp.event_type
      - suricata.snmp.event_type
      - suricata.tftp.event_type
      - suricata.tls.event_type
      - sysmon.NetworkConnection.SourcePortName
      - sysmon.NetworkConnection.DestinationPortName
      - zeek.conn.service
    concepts:
      []
```

Similarly, you can display all models with:

```bash
vast show models --yaml
```

```yaml
- model:
    name: net.connection
    description: ""
    definition:
      - net.src.ip
      - net.src.port
      - net.dst.ip
      - net.dst.port
      - net.proto
```

[taxonomies]: ../../understand/data-model/taxonomies.md
