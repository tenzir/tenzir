---
sidebar_position: 3
---

# Data Aging

When working with data at rest, two big topics that arise from a security
operations perspective:

1. Manage retention span to maximize retrospective detection capabilities
2. Comply with data residency requirements and privacy regulations

Often, these two topics conflict each other: security teams want maximum
retention spans to find advanced attacks retroactively once intelligence becomes
public. The recent watershed events, like SolarWinds and log4j, demonstrate that
post-hoc detection must now span *months to years* to successfully uncover deep
infiltrations. Conversely, security data contains often personal information and
falls under strict regulations that prohibit unfettered use. There exists a
strict upper bound on storing specific security event data for retrospective
detection.

![Data Constraints](data-constraints.excalidraw.svg)

How can we implement a successful detection and response strategy in
this spectrum? By making the spectrum transparent and fully controllable. In
VAST, we developed a declarative compaction approach to perform [fine-grained
transformation](../../use/transform/README.md) of historical data to control
retention span and manage finite storage. Compaction operates in two dimensions:

1. **Temporal**: temporal compaction defines what to do with data as a function
   of age. For example, a policy may dictate a maximum retention of 1 week for
   events containing URIs and 3 months for events containing IP addresses
   related to network connections. However, these retention windows could be
   broadened when pseudonomyzing or anonymizing the relevant fields. VAST not
   only supports deletion of data after exceeding a configured age, but also
   transforming the data with a set of transformations (such as hashing,
   encrypting, permuting). The intuitive declarative definition makes it easy to
   express data residency regulations in a shareable form, e.g., with data
   protection officers.

2. **Spatial**: Traditionally, reaching a storage budget triggers deletion of
   the oldest (or least-recently-used) data. This is a binary decision to throw
   away a subset of events. What if you could only throw away the irrelevant
   parts and keep the information that might still be useful for longitudinal
   investigations? What if you could aggregate multiple events into a single
   one that captures valuable information? Imagine, for example, to halve the
   space utilization of events with network flow information and keeping them 6
   months longer; or imagine you could roll up a set of flows into a traffic
   matrix that only captures who communicated with whom in a given timeframe.

   By incrementally elevating data into more space-efficient representations,
   spatial compaction gives you a much more powerful mechanism to achieve long
   retention periods while working with high-volume telemetry.

:::tip Key Benefits
In summary, VAST's data aging capabilities have the following benefits:

- **Easy Compliance**: VAST makes it easy to implement and share compliance
  policies. The configuration of temporal compaction is a human-readable file
  that clearly states what happens with the data at what age.

- **Gradual Event Decay**: instead of deleting old data as a whole, VAST offers
  an incremental approach to reduce size and information content of events. For
  example, first filter out unneeded fields, then aggregate the smaller events
  into an event summary, and delete the summary at last. Being able to express
  event decay in this gradual way is the Goldilocks approach of managing utility
  of security telemetry.

- **Dynamic Aging**: When only considering age as input for deleting old data,
  high-volume data source also dictate the retention span for low-volume event
  streams, because they can consume orders of magnitude more space. VAST's
  weighted aging makes it possible to define a *relative* importance of events
  to each other. By assigning higher age weights to low-volume-but-important
  data sources (e.g., alerts), we can selectively increase their retention span.
  This unique age prioritization makes it easy to define importance of events
  independent of the event mix.
:::
