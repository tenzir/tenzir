---
title: Tenzir Node v4.21
authors: [raxyte, IyeOnline]
date: 2024-10-04
tags: [release, node]
comments: true
---

Parsing is now easier, faster, and better than before with [Tenzir Node
v4.21][github-release]. Also: introducing an all-new integration with Azure Blob
Storage.

![Tenzir Node v4.21](tenzir-node-v4.21.excalidraw.svg)

[github-release]: https://github.com/tenzir/tenzir/releases/tag/v4.21.0

<!-- truncate -->

## Reading in Fortinet and Sophos logs

Tenzir now supports reading Fortinet and Sophos logs. This feature is powered
by an update to the KV-Parser, which has been upgraded to allow quoted values.
For example, here's how you can use the new parser to take apart a Fortinet
FortiGate log file:

```text {0} title="Parse a Fortinet log file"
from "fortinet.log" read kv
| parse rawdata kv "\|" "="
```

## Parsing Improvements

We’ve made significant improvements to the accuracy of all parsers that deal
inputs of different schemas. Previously, parsers would often attempt to merge
input schemas, which could lead to additional null fields in the data. Now,
parsers strictly adhere to the input data. Basically, you now exactly get out
what you put in.

Alongside this, we also improved the user's control over the schema produced by the
parsers. For example, you can now give a `schema` to the `kv` parser, which gives
you the ability to specify the schema it should produce.

```text {0} title="Parse a Fortinet log file adhering to a manually specified schema"
from "fortinet.log" read kv --schema=my_schema
| parse rawdata kv "\|" "="
| …
```

You can find more information about these new options in [our
documentation](/formats#parser-schema-inference).

## Azure Blob Storage Integration

Tenzir now has an [integration with Azure Blob
Storage](/connectors/azure-blob-storage)! This integration enables users to
securely load log files and export processed data back to Azure Blob Storage,
all while benefiting from both the flexibility and scalability of cloud storage
and the efficiency of Tenzir's data pipelines.

```text {0} title="Load 'suricata.json', authorized as 'tenzirdev'"
from "abfss://tenzirdev@container/suricata.json" read suricata
…
```

```text {0} title="Save events as csv to 'data.csv', authorized as 'tenzirdev'"
…
| to "abfss://tenzirdev@container/data.csv"
```

:::note Authenticate with the Azure CLI
Run `az login` on the command-line to authenticate the current user with Azure's
command-line arguments.
:::

## Other Changes

We have made great progress on TQL2 in the last month. A significant amount of
the language is already ported and a lot new and exciting functionality has been
implemented. You can expect the documentation to be available with the next
release.

This release additionally includes numerous small bug fixes and under-the-hood
improvements. For a detailed list of changes, be sure to check out the
[changelog][changelog].

## Join Us for Office Hours

Every second Tuesday at 8 AM EST / 11 AM EST / 5 PM CET / 9:30 PM IST, we hold
office hours on our Discord server. Whether you have ideas for new packages or
want to discuss upcoming features—join us for a chat!

[discord]: /discord
[changelog]: /changelog#v4210
