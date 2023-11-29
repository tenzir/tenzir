# context apply/update

Applies a context. The contexts defined via this source can be used
with the [`context` source](../sources/context.md).

## Description

The `context` operator manages and applies contexts. Contexts are a flexible
mechanism for data enrichment. A context has a type and is implemented as a
`context` plugin. Each context plugin provides a specific feature set. For
example the `lookup-table` context can be used to extend events with
key-value-based contexts that consist of events passed on to the `context
update <name> key=<field>` operator call.

### `apply`

Applies a context to events in the pipeline, enriching them with the context
entry whenever necessary.

The `enrich` operator is an alias of `context apply`.

#### Synopsis

```
context apply <name> [parameters]
```

### `update`

Updates a context with new data.

#### Synopsis

```
context update <name> [parameters]
```

## Examples

Replace all previous data in the `lookup-table` context "mytable" with data
from the [Feodo Tracker IP
blocklist](https://feodotracker.abuse.ch/downloads/ipblocklist.json), using the
`ip_address` field as the key.

```
from https://feodotracker.abuse.ch/downloads/ipblocklist.json read json
--arrays-of-objects | context update mytable key="ip_address" clear=true
```

Apply the `lookup-table` context "mytable" to events, using the `src_ip` field
as the field to compare the context key against.

```
export | enrich mytable field="src_ip" | where mytable.key != null
```
