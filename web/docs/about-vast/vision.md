---
sidebar_position: 2
---

# Vision

At Tenzir, our vision is that VAST becomes the foundation of a data-first
security operations architecture. Our goal is to break up security silos by
decomposing monolith systems into modular building blocks that can be flexibly
recomposed with a best-of-breed mindset. We commit to open, standardized
interfaces at the core that prevent vendor lock-in, both for security content
(e.g., [STIX][STIX], [CACAO][CACAO]) and structured event data (e.g., [Apache
Arrow][Arrow]).

[STIX]: https://oasis-open.github.io/cti-documentation/stix/intro.html
[CACAO]: http://docs.oasis-open.org/cacao/security-playbooks/v1.1/security-playbooks-v1.1.html
[Arrow]: https://arrow.apache.org

Our conviction is that this requires a fundamentally different approach to
security operations center (SOC) architecture. Conceptually, we need to shift
away from point-to-point product integrations built on top of narrow custom APIs
to an open *security data fabric* as central driver of use cases. This fabric
abstracts away the complexity of the infrastructure and provides connectivity
from cloud to distal on-premise locations, but it also features modular
functions that can easily be composed into use cases in the detection and
response realm. Authorized parties join this fabric to announce their
capabilities. Based on a common ontological understanding, operators merely have
to connect applications to the fabric to yield an autonomously acting system.

The diagram below illustrates the core idea of this architectural shift that
moves away from point-to-point integrations:

![Security Data Fabric](/img/security-data-fabric.light.png#gh-light-mode-only)
![Security Data Fabric](/img/security-data-fabric.dark.png#gh-dark-mode-only)

The network of VAST nodes forms the security data fabric. Communication takes
place over a pluggable messaging *backbone*, such as Kafka, RabbitMQ, or MQTT.
In this architecture, VAST assumes a mediator function, with a backbone-facing
and local-facing side. On the backbone-facing side, VAST implements the security
domain knowledge and analytical processing power to enable composable use cases,
such as routing security content to detection engines, or executing security on
top of the available telemetry. On the local-facing side, VAST integrates
security tools by infusing them with fabric-provided data and, vice versa, and
exposing their key data to the fabric for other tools.

The communication principle of the fabric is publish-subscribe, wrapping
request-response-style communication where appropriate. An example scenario
looks as follows: a network sensor publishes structured events to the fabric. A
detector subscribes to this data stream to publish alerts back to the fabric. A
triaging engine consumes to the alerts, inventory, and vulnerability information
to create prioritized incidents in a case management tool.

:::info OpenDXL Comparison
[OpenDXL](https://www.opendxl.com/) might appear similar in many ways. The key
difference is that we do not want to prescribe MQTT as fixed backbone. While
this may work for some scenarios, [large SOCs often use Kafka][intel-soc] as
their high-bandwidth messaging backbone. In addition, we do not want to burden
operators with rolling out *another* message bus that abstracts the
infrastructure complexity. VAST should just use what is there.

We demonstrated the concept of a pluggable backbone in [Threat
Bus](https://github.com/tenzir/threatbus), which onboards data to the fabric by
converting it to STIX and then routing it via the backbone.
:::

[intel-soc]: https://www.intel.com.au/content/www/au/en/it-management/intel-it-best-practices/modern-scalable-cyber-intelligence-platform-kafka.html

How do we get there? To answer this question, we first have to understand where
we are today.

## The SOC Architecture Maze

Today's SOC architecture is product-centric: SIEM harbors all data, SOAR
executes workflows and calls APIs, TIP manages security content, EDR raises
alerts from the endpoint, NDR from the network, and CDR from the cloud—all
served with metadata where possible. When combined, voilà XDR:

![Traditional SOC Architecture](/img/traditional.light.png#gh-light-mode-only)
![Traditional SOC Architecture](/img/traditional.dark.png#gh-dark-mode-only)

:::warning General Issues
There are several general issues with this approach:

1. **Data Silos**: many security products (especially SaaS) sit in the
   path of the data and capture activity telemetry in their own backend from
   where they drive analytics. However, you can often only see the distilled
   reports without having full access to their own data. Pre-canned analytics
   allow for some processing in a vendor-specific toolchain, and an "open" API
   may allow for selective, low-bandwidth access. But since egress is expensive,
   vendors are incentivised to shield this data from you. A classical silo.
2. **Vendor Lock-in**: after stitching together dozens of different tools,
   you are finally in business, assuming that the strategic alliances programs
   between the vendors exactly implement your use cases. If not? Then you are at
   the mercy of your vendors. But even when you've settled with the existing
   integrations, SOC technology constantly evolves. You will want to integrate
   the next best-in-class solution, and hopefully it plays nicely with the
   existing ones. So how to avoid this gambling? There is always the big-vendor
   monolith security stack: the integrated solution for all your needs. Some
   can live with a fully externally dictated roadmap and cost ratchet, others
   switch from the frying pan to the fire.
3. **Compliance**: public cloud solutions may support choice of geographic
   region for data storage, to meet coarse data residency requirements. This is
   a good start. For on-prem products there appears full control, but is it
   really enough for achieving compliance? How to guarantee that minimum/maximum
   retention spans are properly enforced? Is data anonymized, pseudonymized, and
   encrypted at the needed level of granularity? If the vendor doesn't provide
   sufficient controls, a roadblock lies ahead.

Aside from these general issues, there are also more specific ones with the
above architecture. This concerns [advanced security
teams](/docs/about-vast/target-audience) that strive for full control over the
their detections. These teams operate with a data-first mindset and bring their
own tools for analytics. The SIEM functionality rarely suffices to perform the
desired analysis, and needs to be ETL'ed into a dedicated analytics workbench,
e.g., Spark or Hadoop. This happens typically with recurring over-night jobs,
on demand when something is missing, or fully upstream of the SIEM by putting an
analytics-capable system in the path of the data. But since SIEM has all the
parsers to onboard data sources, this now requires re-implementing data
acquisition partially. Few SOCs have the required data engineering inhouse to
scale this, which leads to buying a *second* SIEM-ish system capable of the
analytics.

[SIEM Offloading](/docs/about-vast/use-cases#siem-offloading) is valid use case,
but it's duct tape from an architectural perspective.
:::

## Security Data Fabric

We envision an alternative architecture to overcome these issues: a **security
data fabric** with analytical processing capabilities in the center of the SOC.

The diagram below outlines the abstraction of the fabric:

![Modern SOC Architecture](/img/modern-approach.light.png#gh-light-mode-only)
![Modern SOC Architecture](/img/modern-approach.dark.png#gh-dark-mode-only)

:::tip Key Benefits

1. **Standardized data access**: unified access to security content, event logs,
   and other high-volume telemetry (e.g., network traffic) streamlines workflows
   across the board. Triaging accuracy and hunting efficiency increases because
   contextualization becomes easier. End-to-end detection engineering becomes
   faster because feature extraction, model validation, and generation of
   security-content uses a single data interface. Response becomes more targeted
   and possible to automate when more data for decision support is present.
2. **Improved collaboration**: given the drastic talent shortage, productivity
   in the SOC is key. In particular, this means efficient collaboration between
   the [central roles](/docs/about-vast/target-audience). When SOC Analyst,
   Detection Engineer, and Data Scientist can work with the same interface,
   their throughput improves across the board, reducing friction raises all
   central MTT* metrics, e.g.,
   mean-time-to-{triage,detect,investigate,respond}.
3. **Sustainable architecture**: when the fabric drives the use cases,
   onboarding new security tools becomes a matter of telling the fabric how to
   expose itself. The byproduct of this indirection is *linear* incremental
   deployability, meaning, you only have to onboard a tool once to the fabric,
   and then all others can interact with it. This also makes for easier
   benchmarking of security tools under evaluation by feeding both the same data
   feed. It also simplifies switching from a legacy tool to a new one, as the
   new one can run ramp up in parallel before turning off the old one. Now we're
   back to best-of-breed with full control.
:::

:::note Security Data Lake
Related is the concept of a **security data lake**. The difference is that the
fabric goes beyond analytic processing of security data and enables use cases
across tools by providing a standardized communication medium. The fabric may
leverage one or more lakes, but the primary focus is on providing unified access
to federated security services.
:::
