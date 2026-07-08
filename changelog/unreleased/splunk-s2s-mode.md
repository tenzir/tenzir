---
title: Send events over Splunk's S2S protocol with `to_splunk mode="s2s"`
type: feature
authors:
  - zedoraps
  - claude
prs:
  - 6430
created: 2026-07-08T08:05:29.176280Z
---

The `to_splunk` operator now speaks Splunk's native forwarding protocol,
Splunk-to-Splunk (S2S), as an alternative transport to HEC. Pass
`mode="s2s"` together with a `<host>:<port>` endpoint to stream events
directly to a Splunk receiver's cooked TCP input (usually port 9997) — the
same port that Universal Forwarders use. This lets Tenzir slot into existing
Splunk ingestion topologies without enabling HEC or managing tokens:

```tql
from {message: "Hello, Splunk!"}
to_splunk "indexer.example.com:9997", mode="s2s",
  index="main", sourcetype="tenzir:json"
```

The familiar options work in both modes: `event`, `raw`, `host`, `source`,
`sourcetype`, `index`, `time`, `fields`, and `include_nulls`. Three new
options tune the S2S transport: `connect_timeout`, `write_timeout`, and
`batch_size`. Note that S2S
truncates timestamps to whole seconds, and `fields` become indexed fields via
Splunk's `_meta` mechanism.

The operator reconnects with exponential backoff when the receiver goes away
and resends the current batch, so delivery is best-effort without hard
guarantees: a resent batch can duplicate events (Splunk receivers do not
deduplicate), and events that were already handed to the network stack when a
receiver crashed can be lost. Exactly-once delivery requires indexer
acknowledgements, which are not yet supported. S2S is a proprietary Splunk
protocol; we verified indexing end-to-end against Splunk Enterprise 8.2
through 10.4. TLS, authentication tokens, and compression are not yet
supported either.
