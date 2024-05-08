# Why Tenzir

On this page we explain what Tenzir is, what you can do with it, and how it
compares to other systems out there.

## What is Tenzir?

Tenzir is a security data pipeline solution for security teams, providing the
following abstractions:

- Tenzir's **pipelines** consist of powerful operators that perform computations
  over [Arrow](https://arrow.apache.org) data frames. The [Tenzir Query Language
  (TQL)](language.md) makes it easy to express pipelines—akin to Splunk and
  Kusto.
- Tenzir's indexed **storage engine** persists dataflows in an open format
  ([Parquet](https://parquet.apache.org/) &
  [Feather](https://arrow.apache.org/docs/python/feather.html)) so that you can
  access them with any query engine, or run pipelines over selective historical
  workloads.
- Tenzir **nodes** offer a managed runtime for pipelines and storage.
- Interconnected nodes form a **data fabric** and pipelines can span across them
  to implement sophisticated security architectures.

![Tenzir Building Blocks](/img/architecture-nodes.excalidraw.svg)

## What can I do with Tenzir?

Use Tenzir if you want to:

- Filter, shape, and enrich events before they hit your SIEM or data lake
- Normalize, enrich, aggregate, and deduplicate structured event data
- Store, compact, and search event data in an open storage format
- Operationalize threat intelligence for live and retrospective detection
- Build your own security data lake
- Create a federated detection and response architectures

## Tenzir vs. X

Tenzir fills a gap in the market—powerful enough for data-intensive security use
cases, but easy enough for security users that are not data engineers.

![Spectra #width500](about/spectra.excalidraw.svg)

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

Tenzir aims to fill this gap as an open pipelines and storage engine for
building scalable security architectures. Pipelines make it easy to transport,
filter, reshape, and aggregate security events, whereas the embedded storage and
query engine enables numerous detection and response workloads to move upstream
of SIEM for a more cost-effective and scalable implementation. Tenzir is built
using open standards, such as Apache Arrow for data in motion and Apache Parquet
for data at rest, preventing vendor lock-in and promote full control of your
event data and security content.

### Tenzir vs. Cribl

[Cribl](https://cribl.io) is a data engine for IT and security. Due to popular
demand, we created a dedicated page where we compare [Tenzir vs.
Cribl](tenzir-vs-cribl.md).

### Tenzir vs. SIEMs

Traditional SIEMs support basic search and a fixed set of analytical operations.
For moderate data volumes, the established SIEM use cases perform well. But when
scaling up to high-volume telemetry data, traditional SIEMs fall behind and
costs often spiral out of control. Traditional SIEMs also lack good support for
threat hunting and raw exploratory data analysis. That's why more advanced use
cases, such as feature extraction, model training, and detection engineering,
require additional workbenches built on data lakes.

Tenzir *complements* a [SIEM][siem] nicely with the following use cases:

- **Offloading**: route the high-volume telemetry to Tenzir that would otherwise
  overload your SIEM or be cost-prohibitive to ingest. By keeping the bulk of
  the data in Tenzir, you remove bottlenecks and can selectively forward the
  activity that matters to your SIEM.

- **Compliance**: Tenzir supports fine-grained retention configuration to meet
  [GDPR](https://en.wikipedia.org/wiki/General_Data_Protection_Regulation) and
  other regulatory requirements. When storage capacity needs careful management,
  Tenzir's *compaction* feature allows for [weighted
  ageing](user-guides/transform-data-at-rest.md) of your data, so that you can
  specify relative importance of event types. Tenzir's powerful pipelines allow
  you to anonymize, pseudonymize, or encrypt specific fields—either to sanitize
  [PII data](https://en.wikipedia.org/wiki/Personal_data) on import, or ad-hoc
  on export when data leaves a pipeline or node.

- **Data Science**: The majority of SIEMs provide an API-only, low-bandwidth
  access path to your security data. Tenzir is an [Arrow][arrow]-native engine
  with unfettered high-bandwidth access so that you can bring demanding
  workloads to your. Bring your own tools, e.g., to run iterative clustering
  algorithms, perform feature extraction, compute embeddings, and perform
  in-stream model inference.

[siem]: https://en.wikipedia.org/wiki/Security_information_and_event_management
[arrow]: https://arrow.apache.org

:::note Recommendation
Unlike a heavy-weight legacy SIEM, Tenzir is highly embeddable so that you can
run it everywhere: containerized in the public cloud, in the data center in the
private cloud, on bare-metal appliances deep in the network, or at the edge.
:::

### Tenzir vs. Databases

Tenzir comes with a builtin storage engine at every node. You may wonder: should
I use an existing database instead?

#### Tenzir vs. Data Warehouses

Data warehouses and
[OLAP](https://en.wikipedia.org/wiki/Online_analytical_processing) engines
seem like an appealing choice for immutable structured data. They offer
sufficient ingest bandwidth, perform well on aggregations, come frequently with
advanced operations like joins, and often scale out well.

However, as a cornerstone for security operations, they fall short in supporting
the following relevant use cases where Tenzir has the edge:

- **Data Onboarding**: it takes considerable effort to write and maintain
  schemas for the tables of the respective data sources. As a data pipeline
  product, Tenzir is well suited for your extract-transform-load (ETL) needs:
  purpose-built for security data, available integrations for key security
  tools, and many data connectors out of the box.

- **Rich Typing**: modeling security event data with a generic database often
  reduces the domain-specific values to strings or integers, as opposed to
  retaining their original semantics, such as IP addresses or port numbers.
  Tenzir offers a rich type system that can retain such semantics during data
  onboarding, giving you the ability to query the data with your own taxonomy at
  query time.

:::note Recommendation
Data warehouses may be well-suited once data is fully structured, but you still
need to cover the ETL and reverse-ETL aspects. Thanks to automatic schema
inference, Tenzir significantly reduces the cost of data onboarding for data
warehouses.
:::

#### Tenzir vs. Relational DBs

Unlike [OLAP](#tenzir-vs-data-warehouses) workloads,
[OLTP](https://en.wikipedia.org/wiki/Online_transaction_processing) workloads
have strong transactional and consistency guarantees, e.g., when performing
inserts, updates, and deletes. These extra guarantees come at a cost of
throughput and latency when working with large datasets, but are rarely needed
for analytical workloads. In a world of incomplete data, high data velocity, and
immutable representations of activity, relational databases are rarely
encountered.

:::note Recommendation
If you aim to perform numerous modifications on a small subset of event data,
with medium ingest rates, relational databases (like PostgreSQL or MySQL), might
be a better fit. Tenzir's columnar data representation is ill-suited for
row-level modifications, but shines for analytical workloads.
:::

#### Tenzir vs. Document DBs

Document DBs, such as MongoDB, offer worry-free ingestion of unstructured
data. They scale well horizontally and flexible querying.

However, they might not be the best choice for the data plane in security
operations, for the following reasons:

- **Vertical Scaling**: when co-locating a storage engine next to high-volume
  data sources, e.g., on a network appliance together with a network monitor,
  CPU and memory constraints coupled with a non-negligible IPS overhead prohibit
  scaling horizontally to build a "cluster in a box."

- **Analytical Workloads**: the document-oriented storage does not perform well
  for analytical workloads, such as aggregations. But such workloads are common
  when hunting or when deploying detections.

- **Economy of Representation**: security telemetry data exhibits a lot of
  repetitiveness between events, such as similar IP addresses, URL prefixes, or
  log message formats. This data compresses much better when transposed into a
  columnar format, such as Parquet.

A special case of document DBs are full-text search engines, such as
ElasticSearch. The unit of input is typically unstructured text. These engines
use (inverted) indexes and ranking methods to return the most relevant results
for a given combination of search terms.

:::note Recommendation
Most of the security tools work with structured data. Operators spend a
substantial amount of time to convert data from unstructured to structured. But
if your primary use case involves working with text documents, Tenzir might not
be a good fit. That said, needle-in-haystack search and other information
retrieval techniques are still relevant for security analytics, for which
Tenzir's indexed storage engine also has basic support.

Tenzir combines the flexibility of the document-oriented data model with the
power of structured OLAP. By relying internally on columnar data representation
with Apache Arrow and simultaneously performing schema inference, you get the
best of both worlds.
:::

#### Tenzir vs. Timeseries DBs

Timeseries databases share a lot in common with [OLAP
engines](#tenzir-vs-data-warehouses), but focus their data organization around
time as key dimension.

:::note Recommendation
If you plan to access your event data only through the time dimension and need
to model the majority of data as series, a timeseries DBs may suit the bill. If
you access data through other (spatial) attributes, like IP addresses or
domains, a traditional timeseries DB might not be good fit—especially for
high-cardinality attributes. If your workloads involve running more complex
detections, or include needle-in-haystack searches, Tenzir might be a better
fit.
:::

#### Tenzir vs. Key-Value DBs

A key-value store performs a key-based point or range lookups to retrieve one or
more values. Security telemetry is high-dimensional data and there are many more
desired entry points than a single key besides time, e.g., IP address,
application protocol, domain name, or hash value.

:::note Recommendation
Key-value stores alone are not suitable as foundation for running security
analytics workloads. There are narrow use cases where key-value stores can
facilitate certain capabilities, e.g., when processing watch lists. (Tenzir
offers a *context* plugin for this purpose.)
:::

#### Tenzir vs. Graph DBs

Graph databases are purpose-built for answering complex queries over networks of
nodes and their relationships, such as finding shortest paths, measuring node
centrality, or identifying connected components. While networks and
communication patterns can naturally be represented as graphs, traditional
security analytics query patterns may not benefit from a graph representation.

:::note Recommendation
If graph-centric queries dominate your use case, Tenzir is not the right
execution engine. Tenzir can still prove valuable as foundation for graph
analytics by storing the raw telemetry and feeding it (via Arrow) into graph
engines that support ad-hoc data frame analysis.
:::

#### Tenzir vs. Vector DBs

Vector databases operate on embeddings, which are high-dimensional floating
point vectors. For generative AI applications, decision support systems, or
search on unstructured data, embeddings are the building block, and vector
databases offer native operations on them, such as approximate nearest neighbor
search.

:::note Recommendation
Tenzir can efficiently represent embeddings via Apache Arrow, but lacks specific
processing capabilities. Use Tenzir to transport your vectors to more
purpose-built engines that also build on Arrow data frames.
:::
