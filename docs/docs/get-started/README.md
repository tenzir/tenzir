# Get Started

VAST is an embeddable security telemetry engine for structured event data.
Tailor-made for security operations, VAST is the foundation for many data-driven
detection and response uses cases, such as operationalizing threat intelligence,
threat hunting, event contextualization, and advanced detection engineering.

:::tip Wanna give it a try?
The [quick start guide](/docs/get-started/quick-start-guide) shows you how to
get up and running and gives a quick tour of the key use cases.
:::

The documentation has the following key sections:

1. [Setup VAST](/docs/setup-vast) describes how you can download, install, and
   configure VAST in a variety of environments.
   👉 *Start here if you want to deploy VAST.*
2. [Use VAST](/docs/use-vast) explains how to work with VAST, e.g., ingesting
   data, running queries, matching threat intelligence, or integrating it with
   other security tools.
   👉 *Go here if you have a running VAST, and want to explore what you can do
   with it.*
3. [Understand VAST](/docs/understand-vast) describes the system design goals
   and architecture, e.g., the actor model as concurrency and distribution
   layer, separation of read/write path, and core components like the catalog
   that provides light-weight indexing and manages schema meta data.
   👉 *Read here if you want to know why VAST is built the way it is.*
4. [Develop VAST](/docs/develop-vast) provides developer-oriented resources to
   customize VAST, e.g., write own plugins or enhance the source code.
   👉 *Look here if you are ready to get your hands dirty and write code.*

If you're unsure whether VAST is the right tool for your use case, keep reading
down below.

## Why VAST?

VAST fills a gap when you need a highly embeddable database for security
data that powers detection and response use cases. The following graphic
illustrates the placement of VAST in the spectrum of *Observability* ⇔
*Security* and *Open Platform* ⇔ *Data Silo*.

![VAST Spectra](/img/ecosystem.png)

We compare VAST to SIEM and data warehouses next, but skip a comparison with
metrics services that we deem out of scope.

### VAST vs. SIEM

VAST *complements* a [SIEM][siem] nicely with the following use cases:

- **Offloading**: route the high-volume telemetry to VAST that would otherwise
  overload your SIEM or be cost-prohibitive to ingest. By keeping the bulk of
  the data in VAST, you remove bottlenecks and can selectively forward the
  activity that matters to your SIEM.

- **Compliance**: VAST has fine-grained retention span configuration to meet
  [GDPR](https://en.wikipedia.org/wiki/General_Data_Protection_Regulation) and
  other regulatory requirements. When storage capacity needs careful management,
  VAST's *compaction* feature allows for weighted ageing of your data, so that
  you can specify relative importance of event types. Powerful *transforms*
  allow you to anonymize, pseudonymize, or encrypt specific fields—either to
  sanitize [PII data](https://en.wikipedia.org/wiki/Personal_data) on import, or
  ad-hoc on export when data leaves VAST.

- **Data Science**: The majority of SIEMs provide an API-only, low-bandwidth
  access path to your security data. VAST is an [Arrow][arrow]-native engine
  that offers unfettered high-bandwidth access so that you can bring your own
  workloads, with your own tools, e.g., to run iterative clustering algorithms
  or complex feature extraction in conjunction with machine learning.

Unlike a heavy-weight legacy SIEM, VAST is highly embeddable so that you can
run it everywhere: containerized in the public cloud, on bare-metal appliances
deep in the network, or at the edge.

[siem]: https://en.wikipedia.org/wiki/Security_information_and_event_management
[arrow]: https://arrow.apache.org

### VAST vs. Data Warehouses

Data warehouses,
[OLAP](https://en.wikipedia.org/wiki/Online_analytical_processing) engines, and
time series databases seem like an appealing choice for immutable structured
data. They offer sufficient ingest bandwidth, perform well on group-by and
aggregation queries, come frequently with advanced operations like joins, and
often scale out well.

However, as a cornerstone for security operations, they fall short in supporting
the following relevant use cases where VAST has the edge:

- **Data Onboarding**: it takes considerable effort to write and maintain
  schemas for the tables of the respective data sources. Since VAST is
  purpose-built for security data, integrations for key data sources and data
  carriers exist out of the box.

- **Rich Typing**: modeling security event data with a
  [COTS](https://en.wikipedia.org/wiki/Commercial_off-the-shelf) database often
  reduces the values to strings or integers, as opposed to retaining
  domain-specific semantics, such as IP addresses or port numbers. VAST offers a
  rich type system that can retain such semantics, supporting both
  *schema-on-read* (taxonomies) and *schema-on-write* (transforms).

- **Fast Search**: typical query patterns are (1) automatically triggered point
  queries for tactical threat intelligence, arriving at a high rate and often in
  bulk, of which the majority are true negatives, (2) group-by and aggregations
  when hunting for threats, or when performing threshold-based detections.

Bottom line: data warehouses may be well-suited for raw data processing, but
a data backbone for security operations has a lot more domain-specific
demands. The required heavy lifting to bridge this gap is cost and time
prohibitive for any security operations center. This is why we built VAST.
