---
sidebar_position: 3
---

# Create a Security Data Fabric

Building a distributed detection and response architecture is a daunting
challenge. Large-scale organizations with strict data residency feel the pain of
extracting insight from disparate data locations. So do multi-cloud environments
and those operating intricate on-premise or private cloud deployments.

*Centralization* of all security data is not always feasible, nor is it
well-defined. Especially managed security service providers (MSSPs) and managed
detection and response (MDR) vendors face this problem due to their needs to
integrate heterogeneous data sources and simultaneously guarantee strict data
segregation per tenant.

Wouldn't it be nice if we can put a layer on top that abstracts the dispersed
infrastructure? This is where the idea of *fabric* comes into play. We use the
term *security data fabric* to refer to a federated, decentralized security data
architecture.[^1] We like to visualize this concept as a data plane when
emphasizing the dataflow, but the distributed nature also requires a management
control plane. A data fabric is at the other spectrum of a [data
lake](build-a-security-data-lake.md), but the two architectual approaches can
complement each other.

![Security Data Plane](security-data-plane.excalidraw.svg)

Tenzir's helps you to logically achieve such an abstraction layer through a
network of pipelines that can be centrally managed. This allows you to implement
security use cases in a federated manner, such as threat hunting, enrichment,
detection, investigation, and incident response.

:::tip Key Takeaways
- The tight coupling of storage and compute of traditional systems makes them
  hard to deploy in a distributed setting.
- Tenzir is a good fit for such complex environments due to the ability to
  deploy nodes close to the data.
- Tenzir offers numerous pipeline operators that serve as building blocks for
  executing detections and running your own analytics—both in-stream and on
  historical data.
- Tenzir's temporal compaction makes it possible to implement data residency and
  sharing policies using pipelines.
- Tenzir's spatial compaction makes it possible to get the most retention span
  out of your data, which is especially relevant in space-confined environments,
  such as appliances.
:::

[^1]: For logs specifically, Gartner speaks about [Federated Security Log
    Management (SLM)](https://www.gartner.com/document/4017131).

## Problem: dispersed infrastructure is hard to secure

Building out distributed security operations in a large-scale infrastructure is
hard because of multiple reasons: a strong coupling of data acquisition and
analytics, a complex heterogeneous tool landscape, costly centralizing of
telemetry, and policies and regulations that prevent data from moving in the
first place.

### Costly and impractical data collection

High-velocity data sources generate immense amounts of telemetry such that costs
quickly spiral out of control:

1. Moving data strains network bandwidth and the incurred transfer costs need to
   be factored into the total cost of ownership (TCO).
2. Ingesting all data into a volume-priced sink is uneconomical. (See example
   calculation below.)
3. Coping with high data rates often requires substantial performance tuning,
   forces to scale horizontally (if feasible at all), and strains the equally
   scarce resources of operations teams.
4. Data overload during investigations or threat hunting can cause a poor
   signal-to-noise ratio, making analysts less productive.

For example, a networking monitor producing 100k events per second (EPS)
accumulates roughly 4 TB/day, assuming an average event size of 500 bytes.
Endpoint telemetry in larger organizations (10–100k employees) can produce
similar amounts. Moving 4 TB/day may incur significant costs.[^2] Splunk clocks
in at $80k/year for 100 GB/day at the [AWS
marketplace](https://aws.amazon.com/marketplace/pp/prodview-jlaunompo5wbw). In a
simple back-of-the-envelope calculation, we'd pay 40x that , i.e., $3.2M. Yikes!

[^2]: In-flight compression is often a necessity. We observed up to 80%
    reduction when using Zstd on JSON-encoded network monitor logs, e.g., 4 TB
    down to 800 GB.

### Strong coupling of data collection & analytics prevents decentralization

Traditional log management and Security Information and Event Management (SIEM)
systems have a strong coupling of data collection and security analytics,
requiring first centralization before analytics can run. This system design may
boost performance in a highly integrated setup, but complicates the deployment
in distributed settings where it yields multiple isolated silos.[^3]

[^3]: [Security data lakes](build-a-security-data-lake.md) have this decoupling
    by design. But lakes don't come with the turnkey security integrations. They
    neither run decentralized yet, although *cloud on-prem* solutions like
    [Google Anthos](https://cloud.google.com/anthos), [AWS
    Outpost](https://aws.amazon.com/outposts/), and [Azure
    Stack](https://azure.microsoft.com/en-us/products/azure-stack) are emerging.

### No BYO analytics due to vendor lock-in

The tight coupling of data collection and analytics in traditional SIEMs causes
another problem: users are now limited to the one console for all of their
analyses. But more advanced detection engineering use cases, such as feature
extraction, model training, backtesting, etc., require high-bandwidth access to
the raw data. Most SIEMs do not expose such raw access and restrict users to a
set of pre-canned operations. For example, it's impossible to use a Python
notebook with custom analytics built on Pandas (or Polars) and run those on the
data.

### Data residency requirements create architectural fragmentation

Data residency policies can prevent centralizing data, forcing data to remain
where it was generated.

Anonymization and pseudonymization can alleviate these restrictions, allowing
for shipping a small substrate back to a central service provider for analysis.
For example, redacting or permuting IP addresses, URLs, or other personal data
may suffice to lift a tight data lockdown. In the case of a security finding,
the investigation can then proceed on the raw data—albeit by again bringing the
investigation to the data.

Such tight compliance and regulatory environments make it hard to operate in the
traditional model of collecting data from remote locations and analyzing it
centrally.

### Tool and data diversity cause detection incompatibilities

Unlike a central data repository where the collection architecture enforces a
unified event taxonomy (or schema) at write time, a distributed architecture is
substantially harder to keep synchronized. Multiple sites not only use different
data naming conventions but also entirely different tools.

## Data fragmentation creates new requirements

These problems require to rethink security operations in a distributed settings
where data is fragmented and spread across numerous islands. While a centralized
architecture focuses on bringing the data to the single place of computation, a
decentralized architecture must bring the computation to the data. Traditional
tools have not been designed to work with this inversion.

**Practicing IaC and DaC**. Operationalizing detection content now requires
pushing it to the edge, so that the distal locations always have an up-to-date
representation of the threat landscape. But this requires a solid management
layer. Security teams often build inhouse fleet management solutions, although
developing and operating this control plane is in itself a sizeable effort. The
problem conceptually resembles managing agents on endpoints, with the added
complexity of sizeable data *at rest*. Operating such an environment requires
strong infrastructure-as-code (IaC) and detection-as-code (DaC) practices, and
thus engineering-centric security teams.

**Standardizing detections**. A heterogeneous data landscape makes detection
engineering substantially harder. [Sigma](https://sigmahq.io) is a great
direction towards normalizing detections to decouple them from multiple data
stores, but it has a narrow scope that focuses on search. Security content is
more than just a search expression, but also includes threat intelligence,
Python scripts, or machine learning models.

**Standardizing data**. Standardizing detections is one angle to reduce
complexity in a heterogeneous environment. Standardizing the data is another.
The [Open Cybersecurity Schema Framework](https://schema.ocsf.io) attempts to
provide a canonical form for security events. However, reshaping all data to
match the OCSF structure requires a powerful translation and validation engine.
Today, security tools do not generate OCSF natively and public mappings are
scarce. The OCSF project is also still fledgling and it remains to be seen
whether it will dominate longer established taxonomies in the community, such as
Elastic Common Schema (ECS).

Nonetheless, standardizing the data shape holds promise to execute a wide range
of detections across data from various vendors. But aren't we now back full
circle to log management and SIEM? *The key difference is the need to operate in
a decentralized fashion to meet the challenges of a distributed environment.*
This requires:

1. Decoupled storage from compute to bring the analytic to the data.
2. Ad-hoc reshaping of data to fit the security content at hand.
3. Flexibly deploying an execution engine to run a existing data.
4. A control plane to disseminate content and manage the infrastructure.

## Solution: Tenzir as a security data fabric

Despite the complex constraints of large-scale infrastructure, Tenzir's
deployment and execution model fits well to realize a federated detection and
response architecture. Tenzir nodes can easily run at multiple locations,
bringing intelligent storage and detection execution capabilities to even distal
parts of the infrastructure.

### Save costs by avoiding eager centralization

A Tenzir deployment saves costs by making it possible keep data close to its
origin, avoiding unneeded transfers. Tenzir pipelines acquire, normalize, and
aggregate data in motion, and further compact data at rest—all on top of open
storage standards (Apache Parquet & Feather) suitable for low-cost object stores
or space-constrained form factors, such as appliances.

By making conscious decisions about forwarding only the subset truly needed for
for global correlation, you can avoid the money trap of a rigid
pay-first-for-ingest-then-do-work architecture. For example, compliance or
forensic readiness use cases may not require centralizing and it suffices to
store the data on low-cost blog storage with a certain retention span.

![Decentralized vs. Centralized](decentralized-vs-centralized.excalidraw.svg)

### Decouple data collection from security analytics

Tenzir's data pipelines provide a toolbox of composable building blocks to
collect, filter, shape, store, and route security data. Acquiring data through
Tenzir has the advantage of solving half of the equation: by relying on open
in-motion and at-rest formats (Apache Arrow and Parquet/Feather), Tenzir enables
choice of the other half: be it a SIEM, a lake, or notebook running a custom
engine.

Raw data access is especially important for detection engineering. Feature
extraction, model training, backtesting, etc., all require high-bandwidth access
to the data.

### Bring Detections to the Data

In a decentralized architecture, a critical capability is bringing the detection
to the data rather than the data to (centralized) detections. Tenzir ships with
ready-made pipeline operators to run detections and analytics, e.g.,
[YARA](../operators/yara.md),
[Sigma](../operators/sigma.md), or
[Python](../operators/python.md). Deploying pipelines at Tenzir
nodes spread over the infrastructure yields a federated execution engine that
can be fueled with detection content.

You can either use Tenzir's built-in operators for expressing computation, or
rely on the Python and C++ plugin interfaces to hook yourself into raw dataflow.
Thanks to highly efficient data architecture, you can run even the most
intricate analytics and expect data to arrive in a standardized format in Apache
Arrow.

### Comply with regulations and use a clearing house

Tenzir helps implement strict data residency requirements with a multi-node
architecture where each node can have independent storage and run distinct
pipelines. Tenzir comes with a managed platform to which nodes connect for
centralized coordinated operation.

With highly flexible data reshaping capabilities at ingest and query time, it is
possible to store the data exactly according to a given policy. Similarly, by
transparently applying pipelines whenever data leaves a node, it is possible to
implement a *clearing house* that sanitizes data when it leaves a regulated
zone, e.g., by encrypting, anonymizing, pseudonymizing, or redacting
information.

For data at rest, Tenzir features a powerful compaction framework. **Temporal
compaction** allows for time-based triggering of transformations, e.g., "apply
the PII scrubbing pipeline after 7 days" or "delete clear-text events after 6
months". **Spatial Compaction** allows for applying arbitrary pipelines as soon
as storage exceeds a given space budget. Tenzir then executes those pipelines
according to schedule that is ordered from the oldest to the youngest data. Age
is relative in that you can assign weights that get multiplied with the original
age. This makes it possible to smartly maintain a storage budget while keeping
relative importance of events.

### Keep data complexity local and increase productivity

By keeping the less actionable data outside the central data repository, less
data pollution occurs and the signal-to-noise ratio increases. Tenzir pipelines
can clean and enriched data before they arrive at a central location, resulting
in higher fidelity workflows.

To address the heterogeneity of data and tools at various locations in a
distributed system, Tenzir has powerful reshaping mechanisms that allow for
mapping data at read time. For example, this allows for ad-hoc transformation
into OCSF with a pipeline operator prior to hitting the data with detection
that requires this data shape.

## Summary

Navigating the complexities of building a distributed detection and response
architecture, especially in large-scale organizations, involves addressing
several critical challenges. These include the high costs and impracticalities
associated with data centralization, the tight coupling of data collection with
analytics in conventional systems, inherent limitations due to vendor lock-in,
diverse data residency regulations, and the difficulties arising from a
heterogeneous landscape of tools and data formats. These challenges make
securing dispersed infrastructures a daunting task.

Tenzir's federated deployment model maps well to complex, distributed
environments. The inherent decoupling of data collection from analytics can lead
to a more flexible and cost-effective management of security data. By deploying
nodes closer to the data's origin, Tenzir facilitates efficient operations in
fragmented and distributed environments. The security data fabric approach marks
a significant shift from traditional centralized systems, offering a scalable,
efficient, and adaptable solution for contemporary, complex infrastructures and
enhancing the effectiveness of security operations.

## References

- Ross Haleliuk. [Security is about data: how different approaches are fighting
  for security data and what the cybersecurity data stack of the future is
  shaping up to look like][haleliuk23-09]. September 25, 2023.

- Anton Chuvakin. [Log Centralization: The End Is
  Nigh?](https://medium.com/anton-on-security/log-centralization-the-end-is-nigh-b28efaa98379). June 30, 2023.

[haleliuk23-09]: https://ventureinsecurity.net/p/security-is-about-data-how-different
