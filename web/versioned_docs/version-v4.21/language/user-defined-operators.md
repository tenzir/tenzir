---
sidebar_position: 4
---

# User-Defined Operators

## Operator Aliases

User-defined operator aliases make pipelines easier to use by enabling users to
encapsulate pipelines into a new operator.

```yaml {0} title="tenzir.yaml"
tenzir:
  operators:
    # Aggregate suricata.flow events with matching source and destination IP
    # addresses.
    summarize-flows:
      where #schema == "suricata.flow"
      | summarize
          pkts_toserver=sum(flow.pkts_toserver),
          pkts_toclient=sum(flow.pkts_toclient),
          bytes_toserver=sum(flow.bytes_toserver),
          bytes_toclient=sum(flow.bytes_toclient),
          start=min(flow.start),
          end=max(flow.end)
        by
          src_ip,
          dest_ip
```

This custom `summarize-flows` operator can now be used in all pipeline
definitions. For example:

```c
/* Write all summarized suricata.flow events to stdout as JSON */
from file path/to/eve.json read suricata
| summarize-flows
| write json
```

:::tip Avoid Recursion
User-defined operators may not reference themselves, but may reference other
user-defined operators. Attempting to use a recursively defined operator in a
pipeline will fail with an error.
:::

## Operator Plugins

In addition to aliases, developers can add additional operators to Tenzir by
using the [operator plugin API](../architecture/plugins.md#operator).
This allows for writing arbitrarily complex operators in C++ by developing
against `libtenzir`.

If you want to learn more about building your own operators, we recommend
studying [Tenzir's built-in operators][builtins-operators], which are developed
against the same API.

[builtins-operators]: https://github.com/tenzir/tenzir/tree/main/libtenzir/builtins/operators
