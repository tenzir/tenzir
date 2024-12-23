# Deduplicate events

The [`deduplicate`](../tql2/operators/deduplicate.md) provides is a powerful
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

```tql
export
where @schema == "zeek.conn"
deduplicate {orig_h: id.orig_h, resp_h: id.resp_h}
```

Providing `id.orig_h` and `id.resp_h` to the operator restricts the output to
all unique host pairs. Note that flipped connections occur twice here, i.e., A →
B as well as B → A are present.

## Remove duplicate alerts

Are you're overloaded with alerts, like every analyst? Let's remove some noise
from our alerts.

First, let's check what our alert dataset looks like:

```tql
export
where @schema == "suricata.alert"
top alert.signature
head 5
```

```tql
{
  alert.signature: "ET MALWARE Cobalt Strike Beacon Observed",
  count: 117369,
}
{
  alert.signature: "SURICATA STREAM ESTABLISHED packet out of window",
  count: 103198,
}
{
  alert.signature: "SURICATA STREAM Packet with invalid ack",
  count: 21960,
}
{
  alert.signature: "SURICATA STREAM ESTABLISHED invalid ack",
  count: 21920,
}
{
  alert.signature: "ET JA3 Hash - [Abuse.ch] Possible Dridex",
  count: 16870,
}
```

Hundreds of thousands of alerts! Maybe I'm just interested in one per hour per
host affected host pair? Here's the pipeline for this:

```tql
from "/tmp/eve.json", follow=true
where @schema == "suricata.alert"
deduplicate {src: src_ip, dst: dest_ip, sig: alert.signature}, timeout=1h
import
```
