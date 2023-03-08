---
sidebar_position: 0
---

# SOC Federation

VAST is an engine for telemetry storage and security content execution,
embeddable at the edge and specifically catering to use cases of larger SOCs at
service providers, network operators, and managed detection and response (MDR)
vendors. Ultimately, [we strive for a federated
architecture](../vision.md) that pushes workloads deep into the edge.
Today, we consider "edge" the constituency that a SOC oversees from a detection
and response perspective.

Before discussing more specific use cases, we discuss our view on edge security
architecture more generally. The diagram below shows an example environment:

![Edge Nodes](edge-nodes.excalidraw.svg)

The core idea is to keep the bulk of the data decentralized at their respective
origin, and disseminate security content via a [security data
fabric](../vision.md#security-data-fabric) to the VAST
nodes—effectively shipping compute to the data. The SOC will always need central
management and collaboration, and selective backhauling of data. The
architecture does not exclude this, but the focus is on offloading as much
processing to the edge as possible, including contextualization and
pre-processing.

This approach differs from traditional architectures that centralizes *all*
security event data, with SIEMs as work horse. Specifically, when building a
central solution on top of legacy SIEMs, operators struggle with (i) keeping up
with ingesting the exponentially increasing data volumes, i.e., operate under a
continuously saturated write path, (ii) answering low-latency queries needed for
ad-hoc investigations and threat hunting, (iii) automating processing of threat
intelligence [live and retrospectively](unified-detection), and (iv)
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
