---
sidebar_position: 4
---

# Show available schemas

When you write a pipeline, you inevitably reference field names from records. If
you do not know the shape of your data or if the data is highly dynamic, you can
introspect the available [schemas](../data-model/schemas.md).

The equivalent of the `SHOW TABLES` of many databases is `show schemas`:

```bash
tenzir 'show schemas | write yaml'
```

```yaml
# Excerpt only
- zeek.conn:
    record:
      - ts:
          timestamp: time
      - uid:
          type: string
          attributes:
            index: hash
      - id:
          zeek.conn_id:
            record:
              - orig_h: ip
              - orig_p:
                  port: uint64
              - resp_h: ip
              - resp_p:
                  port: uint64
      - proto: string
      - service: string
      - duration: duration
      - orig_bytes: uint64
      - resp_bytes: uint64
      - conn_state: string
      - ...
```

This example shows the schema for a Zeek conn.log. You can see the various
fields as list of key-value pairs under the `record` key. The nested record
`id` that is a type alias with the type name `zeek.conn_id`.
