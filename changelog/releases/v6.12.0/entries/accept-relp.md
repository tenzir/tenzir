---
title: Receive Syslog over RELP
type: feature
authors:
  - mavam
created: 2026-08-16T12:00:00Z
---

The new `accept_relp` operator receives Syslog over the Reliable Event Logging Protocol (RELP), including plaintext, TLS, and mutual TLS connections. It preserves complete RELP message boundaries and acknowledges each event after accepting it for processing:

```tql
accept_relp "0.0.0.0:2514"
syslog = data.parse_syslog()
```

An acknowledgement confirms that Tenzir accepted the event into a bounded in-memory handoff, not that a downstream destination stored it. RELP provides at-least-once transport, so the client may resend an event if the connection fails before it receives the acknowledgement.
