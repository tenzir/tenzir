# FAQs

This page provides answers to frequently asked questions (FAQs) about Tenzir.

## What is Tenzir?

Tenzir is the data pipeline engine for security teams, enabling you to
collect, parse, shape, normalize, aggregate, route, store, and query your
security telemetry at ease.

Tenzir is also the name of the startup behind the product.

## What part of Tenzir is open and what part is closed source?

To create a healthy open core business with a thriving open source foundation,
we aim to find the right balance between enabling open source enthusiast whilst
offering a commercial package for those seeking a turn-key solution:

![Open source vs. closed source](open-vs-closed-source.svg)

There exist three moving parts:

1. **Executor**: The pipeline executor is open source and available under a
   permissive BSD 3-clause licence at
   [GitHub](https://github.com/tenzir/tenzir).
2. **Node**: The node makes pipeline management easy. A node orchestrates
   multiple pipelines and offers additional features, such as contexts for
   enrichment and an indexed storage engine.
3. **Platform**: The platform is the control plane for managing nodes and offers
   a web-based interface.

You can either [build the open source version](development/build-from-source.md)
yourself and [add your own plugins](architecture/plugins.md), or use our
compiled binary packages that include the command line tool and the node. We
offer the platform as Docker Compose files. We host one platform instance at
[app.tenzir.com](https://app.tenzir.com).

Tenzir comes in severals editions, including a free [Community
Edition](https://tenzir.com/pricing). If you have any questions, don't hesitate
to [reach out](https://tenzir.com/contact) what best suits your needs.

## Can Tenzir see my data?

*No*, but let us explain.

A Tenzir deployment consists of *nodes* that you manage, and a *platform*
available as SaaS from us or operated by you. The *app* runs in your browser to
access the platform. All computation and storage takes place at your nodes. The
platform acts as rendezvous point that connects two TLS-encrypted channels, one
from the node to the platform, and one from the browser to the platform:

![Platform Connections](platform-connections.excalidraw.svg)

We connect these two channels at the platform. Therefore, whoever operates the
platform *could* interpose on data that travels from the nodes to the app. In
the [Professional Edition](https://tenzir.com/pricing) and [Enterprise
Edition](https://tenzir.com/pricing), we run the platform. However, we emphasize
that data privacy is of utmost importance to us and our customers. As a
mission-driven company with strong ethics, our engineering follows
state-of-the-art infrastructure-as-code practices and we are performing security
audits to ensure that our code quality meets the highest standards.

We have plans to make this a single, end-to-end encrypted channel, so that we
no longer have the theoretical ability to interpose on the data transfer between
app and node.

If you have more stringent requirements, you can also run the platform yourself
with the [Sovereign Edition](https://tenzir.com/pricing).

## Does Tenzir run on-premise?

Yes, Tenzir can run on premise and support fully air-gapped environments. The
[Sovereign Edition](https://tenzir.com/pricing) allows you to [deploy the entire
platform](installation/deploy-the-platform.md) in a Dockerized environment, such
as Docker Compose.

The [Community Edition](https://tenzir.com/pricing), [Professional
Edition](https://tenzir.com/pricing) and [Enterprise
Edition](https://tenzir.com/pricing) are backed by a Tenzir-hosted instance of
the platform in the public cloud (AWS in Europe):

Read more on [how Tenzir works](how-tenzir-works/README.md) to understand the
key differences.

## Does Tenzir offer cloud-native nodes?

Tenzir currently does not offer cloud-hosted nodes. You can only run nodes in
your own environment, including your cloud environment.

However, we offer a cloud-native *demo node* that you can deploy as part of
every account.

## Why Did You Create a New Query Language? Why Not SQL?

We opted for our own language—the **Tenzir Query Language** or **TQL**— for
several reasons that we outline below.

At Tenzir, we have a clear target audience: security practitioners. They are
rarely data engineers fluent in SQL or experienced with lower-level data tools.
Rather, they identify as blue/purple teamers, incident responders, threat
hunters, detection engineers, threat intelligence analysts, and other domain
experts.

**Why Not Stick with SQL?** SQL, while powerful and pervasive, comes with
significant usability challenges. Despite decades of success, SQL's syntax can
be hard to learn, read, and modify, even for experts. Some of the key
limitations include:

- **Rigid Clause Order**: SQL's fixed structure (e.g., `SELECT ... FROM ... WHERE
  ...`) forces users to think in an "inside-out" manner that doesn’t match the
  natural flow of data transformations. This often leads to complex subqueries
  and redundant clauses.
- **Complex Subqueries**: Expressing multi-level aggregations or intermediate
  transformations typically requires deeply nested subqueries, which hurt
  readability and make edits labor-intensive.
- **Difficult Debugging**: SQL's non-linear data flow makes tracing logic
  through large queries cumbersome, impeding efficient debugging.

These challenges make SQL difficult for security practitioners who need to
focus on quick, intuitive data analysis without getting bogged down by the
intricacies of query structuring.

**Why a Pipeline Language?** We chose a pipeline-based approach for our query
language because it enhances user experience and addresses the pain points of
SQL. Here’s how:

- **Sequential and Intuitive Data Flow**: Pipeline syntax expresses data
  operations in a top-to-bottom sequence, reflecting the logical order of data
  transformations. This makes it easier to follow and understand, especially
  for complex queries.
- **Simplified Query Construction**: With a pipeline language, operations can
  be chained step-by-step without requiring nested subqueries or repetitive
  constructs. This improves readability and allows users to build and modify
  queries incrementally.
- **Easier Debugging**: Each stage in a pipeline can be isolated and inspected,
  simplifying the process of identifying issues or making adjustments. This is
  in stark contrast to SQL, where changes often ripple through multiple
  interconnected parts of a query.

**Lessons from Other Languages.** We spoke to numerous security analysts with
extensive experience using SIEMs. Splunk's Search Processing Language (SPL), for
instance, has set a high standard in user experience, catering well to its
non-engineer user base. This inspired us to create a language with:

- The *familiarity* of [Splunk](https://splunk.com)
- The *power* of [Kusto](https://github.com/microsoft/Kusto-Query-Language)
- The *flexibility* of [jq](https://stedolan.github.io/jq/)
- The *clarity* of [PRQL](https://prql-lang.org/)
- The *expressiveness* of [dplyr](https://dplyr.tidyverse.org/)
- The *ambition* of [SuperPipe](https://zed.brimdata.io/)
- The *composability* of [Nu](https://www.nushell.sh/)

Even Elastic recognized the need for more intuitive languages by introducing
[ES|QL](/blog/a-first-look-at-esql), which leans into a pipeline style. Nearly
every major SIEM and observability tool has adopted some version of a pipeline
language, underscoring the trend toward simplified, step-by-step data
handling.

**Balancing Streaming and Batch Workloads.** One of our key design goals was to
create a language that effortlessly handles both streaming and batch processing.
By allowing users to switch between historical and live data inputs with minimal
change to the pipeline, our language maintains flexibility without introducing
complexity.

Our decision to develop a new language was not taken lightly. We aimed to
build on the strengths of SQL while eliminating its weaknesses, creating an
intuitive, powerful tool tailored for security practitioners. By combining
insights from existing successful pipeline languages and leveraging modern
data standards, we offer a user-friendly and future-proof solution for security
data analysis.

## What database does Tenzir use?

Tenzir does not rely on a third-party database.

Tenzir nodes include a light-weight storage engine on top of partitioned Feather
or Parquet files, accessible via the [`import`](operators/import.md) and
[`export`](operators/export.md) operators. The engine comes with a
catalog that tracks meta data and a thin layer of sketches to accelerate
queries.

Our [tuning guide](installation/tune-performance/README.md) has further details
on the inner workings.

## Does a Tenzir node run on platform *X*?

We currently support the platforms that we mention in our [deployment
instructions](installation/deploy-a-node.md).

For any other platform, the answer is most likely *no*. While we would love to
support a wide variety of platforms, we are still a small team with limited
engineering bandwidth. Please [talk to us](/discord) to let us know what is
missing and consider contributing support for additional platforms to our [open
source project](https://github.com/tenzir/tenzir).

## Do you have an integration for *X*?

Our [integrations page](integrations.md) includes descriptions of use cases
with third-party products and tools. If *X* is not in that list, it does not
mean that *X* is not supported. The steps below help you understand whether
there exists an integration:

1. Check the available [formats](formats.md). Sometimes an integration is just a
   lower-level building block, such as the [Syslog parser](formats/syslog.md).
2. Check the available [connectors](connectors.md). An integration can also be
   generic communication primitive, such as the [AMQP](connectors/amqp.md) that
   acts as client to speak with a RabbitMQ server, or the
   [HTTP](connectors/http.md) connector to perform an API call.
3. Check Fluent Bit [inputs][fluentbit-inputs] and [outputs][fluentbit-outputs].
   Our [`fluent-bit`](operators/fluent-bit.md) operator makes it possible to use
   the entire ecosystem of Fluent Bit integrations.
4. Call a command-line tool. It is always possible to integrate a command line
   tool using the [`shell`](operators/shell.md) operator, by hooking
   standard input and output of a forked child as a byte stream into a
   pipeline.
5. Use Python. The [`python`](operators/python.md) operator allows you to
   perform arbitrary event-to-event transformation using the full power of
   Python.

Please do not hesitate to reach out to us if you think something is missing, by
[opening a GitHub
Discussion](https://github.com/orgs/tenzir/discussions/new/choose) or swinging
by our [Discord server](/discord).

[fluentbit-inputs]: https://docs.fluentbit.io/manual/pipeline/inputs/
[fluentbit-outputs]: https://docs.fluentbit.io/manual/pipeline/outputs/
