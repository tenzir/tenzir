---
sidebar_position: 0
---

# Why VAST

VAST fills a gap in the market—powerful enough for data-intensive security use
cases, but easy enough for security users that are not data engineers.

![Spectra #width500](spectra.excalidraw.svg)

One of the major challenges in cybersecurity is the increasing volume of data
that organizations need to manage and analyze in order to protect their critical
infrastructure and sensitive data from cyber threats. Traditional security
information and event management (SIEM) systems are not designed to scale with
this increasing volume of data, and can become costly and time-consuming to
maintain. Additionally, cloud and data lakes are often geared towards data
engineers rather than security professionals, leading to security teams having
to spend valuable time and resources wrangling data instead of hunting for
threats. This creates a significant barrier for organizations looking to
effectively protect their data and infrastructure from cyber attacks.

What is needed is a security-native data architecture that enables security
teams to take control of their data and easily deploy and manage the flow of
security data. This solution should be able to integrate seamlessly with
existing data architectures, scale from single nodes to highly distributed data
fabrics, and support a variety of deployment options including cloud,
on-premises, decentralized, and even air-gapped environments.

Importantly, a security-native data architecture should be easy to deploy, use,
and manage *without* the need for dedicated data engineering resources. It
should also be built around security standards and integrate easily with
security tools in a plug-and-play fashion.

VAST aims to fill this gap as an open pipelines and storage engine for building
scalable security architectures. Pipelines make it easy to transport, filter,
reshape, and aggregate security events, whereas the embedded storage and query
engine enables numerous detection and response workloads to move upstream of
SIEM for a more cost-effective and scalable implementation. VAST is built using
open standards, such as Apache Arrow for data in motion and Apache Parquet for
data at rest, preventing vendor lock-in and promote full control of your event
data and security content.

## VAST vs. SIEMs

Traditional SIEMs support basic search and a fixed set of analytical operations.
For moderate data volumes, the established SIEM use cases perform well. But when
scaling up to high-volume telemetry data, traditional SIEMs fall behind and
costs often run out of control. Traditional SIEMs also lack good support for
threat hunting and raw exploratory data analysis. That's why more advanced use
cases, such as feature extraction, model training, and detection engineering,
require additional data-centric workbenches.

VAST *complements* a [SIEM][siem] nicely with the following use cases:

- **Offloading**: route the high-volume telemetry to VAST that would otherwise
  overload your SIEM or be cost-prohibitive to ingest. By keeping the bulk of
  the data in VAST, you remove bottlenecks and can selectively forward the
  activity that matters to your SIEM.

- **Compliance**: VAST supports fine-grained retention configuration to meet
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

[siem]: https://en.wikipedia.org/wiki/Security_information_and_event_management
[arrow]: https://arrow.apache.org

:::note Recommendation
Unlike a heavy-weight legacy SIEM, VAST is highly embeddable so that you can
run it everywhere: containerized in the public cloud, in the data center in the
private cloud, on bare-metal appliances deep in the network, or at the edge.
:::

## VAST vs. Data Warehouses

Data warehouses and
[OLAP](https://en.wikipedia.org/wiki/Online_analytical_processing) engines
seem like an appealing choice for immutable structured data. They offer
sufficient ingest bandwidth, perform well on group-by and aggregation queries,
come frequently with advanced operations like joins, and often scale out well.

However, as a cornerstone for security operations, they fall short in supporting
the following relevant use cases where VAST has the edge:

- **Data Onboarding**: it takes considerable effort to write and maintain
  schemas for the tables of the respective data sources. Since VAST is
  purpose-built for security data, integrations for key data sources and data
  connectors exist out of the box.

- **Rich Typing**: modeling security event data with a generic database often
  reduces the values to strings or integers, as opposed to retaining
  domain-specific semantics, such as IP addresses or port numbers. VAST offers a
  rich type system that can retain such semantics at ingest time, while also
  giving you the ability to query the data with your own taxonomy at query time.

- **Fast Search**: typical query patterns are (1) automatically triggered point
  queries for tactical threat intelligence, arriving at a high rate and often in
  bulk, of which the majority are true negatives, (2) regular expression search
  for finding patterns in command line invocations, URLs, or opaque string
  messages, and (3) group-by and aggregations when hunting for threats or when
  performing threshold-based detections. Data warehouses work well for (3) but
  rarely for (1) and (2) as well.

:::note Recommendation
Data warehouses may be well-suited for raw data processing, but a data backbone
for security operations has a lot more domain-specific demands. The required
heavy lifting to bridge this gap is cost and time prohibitive for any security
operations center. This is why we built VAST.
:::

## VAST vs. Relational DBs

Unlike [OLAP](#vast-vs-data-warehouses) workloads,
[OLTP](https://en.wikipedia.org/wiki/Online_transaction_processing) workloads
have strong transactional and consistency guarantees, e.g., when performing
inserts, updates, and deletes. These extra guarantees come at a cost of
throughput and latency when working with large datasets, but are rarely needed
in security analytics (e.g., ingestion is an append-only operation). In a domain
of incomplete data, VAST trades correctness for performance and availability,
i.e., throttles a data source with backpressure instead of falling behind and
risking out-of-memory scenarios.

:::note Recommendation
If you aim to perform numerous modifications on a small subset of event data,
with medium ingest rates, relational databases, like PostgreSQL or MySQL, might
be a better fit. VAST's columnar data representation is ill-suited for row-level
modifications.
:::

## VAST vs. Document DBs

Document DBs, such as MongoDB, offer worry-free ingestion of unstructured
data. They scale well horizontally and flexible querying.

However, they might not be the best choice for the data plane in security
operations, for the following reasons:

- **Vertical Scaling**: when co-locating a storage engine next to high-volume
  data sources, e.g., on a network appliance together with a network monitor,
  CPU and memory constraints coupled with a non-negligible IPS overhead prohibit
  scaling horizontally to build a "cluster in a box."

- **Analytical Workloads**: the document-oriented storage does not perform well
  for analytical workloads, such as group-by and aggregation queries. But such
  analytics are very common in interactive threat hunting scenarios and in
  various threshold-based detections. VAST leverages Arrow for columnar data
  representation and partially for query execution.

- **Economy of Representation**: security telemetry data exhibits a lot of
  repetitiveness between events, such as similar IP addresses, URL prefixes, or
  log message formats. This data compresses much better when transposed into a
  columnar format, such as Parquet.

A special case of document DBs are full-text search engines, such as
ElasticSearch or Solr. The unit of input is typically unstructured text. The
search engine uses (inverted) indexes and ranking methods to return the most
relevant results for a given combination of search terms.

:::note Recommendation
Most of the security telemetry arrives as structured log/event data, as opposed
to unstructured textual data. If your primary use case involves working with
text, VAST might not be a good fit. That said, needle-in-haystack search
and other information retrieval techniques are still relevant for security
analytics, for which VAST has basic support.
:::

## VAST vs. Timeseries DBs

Timeseries databases share a lot in common with [OLAP
engines](#vast-vs-data-warehouses), but put center data organization around
time.

:::note Recommendation
If you plan to access your event data through time domain and need to model the
majority of data as series, a timeseries DBs may suit the bill. If you access
data through other (spatial) attributes, like IP addresses or domains, a
traditional timeseries DB might not be good fit—especially for high-cardinality
attributes. If your analysis involve running more complex detections, or
include needle-in-haystack searches, VAST might be a better fit.
:::

## VAST vs. Key-Value DBs

A key-value store performs a key-based point or range lookup to retrieve one or
more values. Security telemetry is high-dimensional data and there are many more
desired entry points than a single key besides time, e.g., IP address,
application protocol, domain name, or hash value.

:::note Recommendation
Key-value stores alone are not suitable as foundation for running security
analytics workloads. There are narrow use cases where key-value stores can
facilitate certain capabilities, e.g., when processing watch lists. (VAST offers
a *matcher* plugin for this purpose.)
:::

## VAST vs. Graph DBs

Graph databases are purpose-built for answering complex queries over networks of
nodes and their relationships, such as finding shortest paths, measuring node
centrality, or identifying connected components. While networks and
communication patterns can naturally be represented as graphs, traditional
security analytics query patterns may not benefit from a graph representation.

:::note Recommendation
If graph-centric queries dominate your use case, VAST is not the right execution
engine. VAST can still prove valuable as foundation for graph analytics by
storing the raw telemetry and feeding it (via Arrow) into graph engines that
support ad-hoc data frame analysis.
:::
