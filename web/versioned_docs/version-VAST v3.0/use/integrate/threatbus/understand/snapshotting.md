---
sidebar_position: 2
---

# Snapshotting

Traditional pub/sub only broadcasts the current de-facto state of a system.
Published messages are either processed by a subscriber or not. Once a message
has passed the bus, it will not be published again.

This is problematic when it comes to security content, such as indicators of
compromise (IOCs). The relevance of IOCs usually spikes shortly after they get
known and then decays over time. In a usual pub/sub system, new subscribers will
not see previously published messages, even though they might still be very
relevant to them.

Threat Bus addresses this with the snapshot feature: **New subscribers can ask
for a historic snapshot of security content.**

## Requesting Snapshots

Requesting a snapshot is part of the subscription interface for clients.
The subscription data structure looks as follows.

```py
@dataclass
class Subscription:
  topic: str
  snapshot: timedelta
```

In case the requested `snapshot` time delta is greater than zero, Threat Bus
forwards the request to all plugins. How this request is handled is up to the
implementing plugin.

## Point-To-Point Forwarding

Instead of publishing requested snapshot data again, Threat Bus uses a
point-to-point transmission model. Only the application that requests a snapshot
gets to see the snapshot. That prevents all other subscribers from eventually
seeing messages more than once.

## Implementation

Snapshotting is implemented by the application plugins. When a new subscriber
asks for a snapshot, Threat Bus forwards the request to all implementing
plugins. Apps **optionally** implement the snapshot feature.

For example, the MISP plugin implements such a handler. When Threat Bus invokes
the handler, the plugin performs a MISP API search for IOCs in the requested
time range. All found items are then passed back to the bus for distribution.
