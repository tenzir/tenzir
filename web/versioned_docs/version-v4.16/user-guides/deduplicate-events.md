# Deduplicate events

The [`deduplicate`](../operators/deduplicate.md) provides is a powerful
mechanism to remove duplicate events in a pipeline.

There are numerous use cases for deduplication, such as reducing noise,
optimizing costs and make threat detection and response more efficent. Read our
[blog post](/blog/reduce-cost-and-noise-with-deduplication) for high-level
discussion.

## Analyze unique host pairs

Let's say you're investigating an incident and would like get a better of
picture of what entities are involved in the communication. To this end, you
would like to extract all unique host pairs to identify who communicated with
whom.

Here's how this looks like with Zeek data:

```
export
| where #schema == "zeek.conn"
| deduplicate id.orig_h, id.resp_h
```

Providing `id.orig_h` and `id.resp_h` to the operator restricts the output to
all unique host pairs. Note that flipped connections occur twice here, i.e., A →
B as well as B → A are present.

## Remove duplicate alerts

Are you're overloaded with alerts, like every analyst? Let's remove some noise
from our alerts.

First, let's check what our alert dataset looks like:

```
export
| where #schema == "suricata.alert"
| top alert.signature
| head 5
```

```json
{
  "alert.signature": "ET MALWARE Cobalt Strike Beacon Observed",
  "count": 117369
}
{
  "alert.signature": "SURICATA STREAM ESTABLISHED packet out of window",
  "count": 103198
}
{
  "alert.signature": "SURICATA STREAM Packet with invalid ack",
  "count": 21960
}
{
  "alert.signature": "SURICATA STREAM ESTABLISHED invalid ack",
  "count": 21920
}
{
  "alert.signature": "ET JA3 Hash - [Abuse.ch] Possible Dridex",
  "count": 16870
}
```

Hundreds of thousands of alerts! Maybe I'm just interested in one per hour per
host affected host pair? Here's the pipeline for this:

```
from /tmp/eve.json --follow
| where #schema == "suricata.alert"
| deduplicate src_ip, dest_ip, alert.signature --timeout 1 hour
| import
```

## Produce a finite amount of retro lookups

The [`lookup`](../operators/lookup.md) operator offers automated live and retro
matching. For every context update, it generates a point query to locate events
with the given value. For frequent data points, e.g., the IP address `127.0.0.1`,
this can create a massive amount of retro hits.

The `deduplicate` operator can avoid potential overload and reduce retro matches
to a constant number of hits. For example, to receive at most 100 hits from a
retrospective lookup, use this pipeline:

```
lookup --retro feodo --field dest_ip
| deduplicate --limit 100 feodo.value
```
