---
sidebar_position: 3
---

# Use Cases

VAST is an engine for telemetry storage and security content execution,
embeddable at the edge and specifically catering to use cases of larger SOCs at
service providers, network operators, and managed detection and response (MDR)
vendors. Ultimately, [we strive for a federated architecture](vision) that
pushes workloads deep into the edge. Today, we consider "edge" the constituency
that a SOC oversees from a detection and response perspective.

Before discussing more specific use cases, we discuss our view on edge security
architecture more generally. The diagram below shows an example environment:

![Edge Nodes](/img/edge-nodes.light.png#gh-light-mode-only)
![Edge Nodes](/img/edge-nodes.dark.png#gh-dark-mode-only)

The core idea is to keep the bulk of the data decentralized at their respective
origin, and disseminate security content via a [security data
fabric](vision#security-data-fabric) to the VAST nodes—effectively shipping
compute to the data. The SOC will always need central management and
collaboration, and selective backhauling of data. The architecture does not
exclude this, but the focus is on offloading as much processing to the edge as
possible, including contextualization and pre-processing.

This approach differs from traditional architectures that centralizes *all*
security event data, with SIEMs as work horse. Specifically, when building a
central solution on top of legacy SIEMs, operators struggle with (i) keeping up
with ingesting the exponentially increasing data volumes, i.e., operate under a
continuously saturated write path, (ii) answering low-latency queries needed for
ad-hoc investigations and threat hunting, (iii) automating processing of threat
intelligence [live and retrospectively](#live--retro-matching), and (iv)
offering high-bandwidth access for data-centric operations like feature
extraction, model training, and detection engineering.

Cloud-native security data lakes may sound like an appealing alternative to
address the scaling issues, but also raise new concerns:

1. **Cloud Choice**: what public-cloud security stack should you choose when
   laying the foundation for SOC service? Most large organizations are
   multi-cloud and it is ineffective to operate *multiple* cloud security
   stacks.
2. **Customization**: if the cloud provider offering fits your needs, great, but
   what about customizations? Are the extension hooks sufficient? What about
   exchanging or integrating a piece of functionality from another vendor who
   offers a more compelling point solution? A best-of-breed strategy is an
   uphill battle in a locked down ecosystem.
3. **On-premise**: private cloud and on-premise deployments will remain next to
   public-cloud workloads. Even though Anthos, Outpost, and Stack bring the
   cloud control plane closer back to the enterprise, certain environments will
   always remain autarkic. To be clear, our goal is to deploy VAST in exactly
   these cloud-on-prem environments, but we strongly believe that no one should
   be *coerced* to run on top of a third-party platform they do not control.

:::tip Key Benefits
A federated edge security architecture with a fleet of VAST nodes as foundation
for detection and response has the following benefits:

1. **Better detection**: as compute ramps up at the edge, so does detection
   capability. Pushing resource-intense workloads (e.g., pre-trained attack models,
   kill-chain state machines, multi-stage analytics) to the edge opens new
   avenues for scaling efficiently.
2. **Faster response**: detections can trigger responses faster due to closer
   proximity to the affected entity. Responding can become smarter and more
   independent when data-driven decision making can rely on low-latency and
   high-bandwidth access to context.
3. **Easier compliance**: when keeping data at the source, adhering to data
   residency requirements becomes simpler. In addition, VAST offers flexible
   transformation and compaction feature to keep data compliant with respect to
   the local policy.
4. **Strong multi tenancy**: A multi-node VAST deployment makes it easy to cater
   to strict data separation while enabling cross-tenant analytics in a
   transparent way. We are working on a clearing house architecture and unified
   fleet management solution to offer a turn-key solution for service providers,
   stay tuned.
:::

Given this architecture, we now take a look at some more specific use cases.

## SIEM Offloading

There is [a trend towards a second SIEM][corelight-2nd-siem], and [it's not
new][gartner-dual-siem]. Benefits include cost savings, new analytical
capabilities, higher visibility, improved detection in a modern engine. And most
importantly, *incremental deployability*: you can add a new system without
disrupting existing services.

![SIEM Offloading](/img/siem-offloading.light.png#gh-light-mode-only)
![SIEM Offloading](/img/siem-offloading.dark.png#gh-dark-mode-only)

When you realize that you need to deploy two SIEMs, you are basically taking the
first step towards a distributed architecture. While it's possible to run the
offloading engine centrally, this is the time to re-evaluate your strategy. How
to comply best with data residency regulations? How do I break down silos? How
can I support threat hunting and detection engineering?

SIEM offloading with a new engine does not mean you have to immediately adopt a
fully decentralized architecture. You can also build your own lakehouse
architecture with VAST, thanks to a standardized data plane via [Apache
Arrow](https://arrow.apache.org). In fact, it makes sense to centralize
heavy-duty analytics that require a lot of horse power. But you can also push a
lot of front-line detection deep into the edge.

[corelight-2nd-siem]: https://corelight.com/blog/one-siem-is-not-enough
[gartner-dual-siem]: https://medium.com/anton-on-security/living-with-multiple-siems-c7fea37c5020

:::tip Key Benefits
Using VAST in front of your SIEM has the following benefits:

1. **Reduced cost**: VAST cuts your bill by absorbing the load of the heavy
   hitters while you can keep using the long tail of integrated data sources
   without disruption.
2. **Higher performance**: VAST's system architecture has a strict separation
   of read and write path that scale independently, making it possible to
   operate the system under continuous inbound load. Compared to legacy SIEMs,
   VAST is a resource-efficient, embeddable telemetry engine that offers 10-100x
   ingestion bandwidth, and executes queries with
   interactive latencies.
3. **Reduce Lock-in**: VAST stores all event data in an open, analytics-friendly
   format ([Parquet](https://parquet.apache.org)) that makes it easy to BYO
   detection workloads.
4. **Easy compliance**: VAST's powerful transforms allow you to perform
   fine-grained field-level modifications to anonymize, pseudonymize, or encrypt
   sensitive data. With compaction, you can specify retention periods (e.g.,
   "anonymize URLs after 7 days") and define a multi-level roll-up strategy to
   age data gracefully.
:::

## Live & Retro Matching

The traditional way of operationalizing security content is *forward-looking*,
by disseminating the content to the detection edge where they can act, e.g.,
network sensors or endpoint agents. If the roll-out relies on periodic pulling,
then there's an inherent minimum delay from the time a detection was available
to the time when it can act. In addition, if there is no historical telemetry at
the edge, the detection must also be applied to the SIEM out-of-band. The
diagram below illustrates this:

![Live Detection](/img/live-detection.light.png#gh-light-mode-only)
![Live Detection](/img/live-detection.dark.png#gh-dark-mode-only)

What we really want is a *unified* approach for operationalizing security
content: automated push-based dissemination with a negligible propagation delay
and installation at the detection edge, so the that future telemetry streams
through the engine. When keeping telemetry at the edge, the new detection should
also immediately trigger a retro scan:

![Live & Retro Detection](/img/live-retro-detection.light.png#gh-light-mode-only)
![Live & Retro Detection](/img/live-retro-detection.dark.png#gh-dark-mode-only)

This architecture decouples the arrival of new security content from the
execution of the detection. VAST runs in production with this use case for
detections in the form of tactical indicators. We are in the process of
extending this mechanism to more advanced stateful detections, e.g., Sigma
correlations or machine-learning models.

:::note Suricon 2021
Check out [our talk][suricon21-slides] with [DCSO](https://dcso.de) at [Suricon
2021](https://suricon.net/suricon-2021-boston/) on how VAST supports this
architecture with Suricata telemetry and security content in the form of STIX
indicators.
:::

[suricon21-slides]: https://github.com/tenzir/events/releases/download/suricon21/slides.pdf
