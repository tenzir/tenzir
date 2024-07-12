# FAQs

This page provides answers to frequently asked questions (FAQs) about Tenzir.

## What is Tenzir?

Tenzir is a platform that pioneers open-source security data pipelines.

Tenzir is also the name of the Germany-based startup behind the product.

## What part of Tenzir is open and what part is closed source?

The diagram below illustrates the high-level components, indicating what parts
are *open* and what are *closed* source:

![Open source vs. closed source](open-vs-closed-source.excalidraw.svg)

The majority of code is open source and available at under a permissive BSD
3-clause licence, available at [GitHub](https://github.com/tenzir/tenzir). This
code implements the `tenzir` and `tenzir-node` [command line
tools](configuration.md) that execute pipelines and store data. A flexible
[plugin infrastructure](architecture/plugins.md) allows for enhancing the
open-source core with closed-source plugins.

The Tenzir stack also consists of a cloud platform and web app, accessible via
[app.tenzir.com](https://app.tenzir.com). Both of these components are not
openly available. The platform is the *control plane* that manages the fleet of
nodes and their pipelines, whereas the nodes are the *data plane* for compute
and storage.

We offer the [Community Edition](https://tenzir.com/pricing) as a binary package
that consists of all open-source plugins, plus our own additional closed source
plugins, such as a plugin that connects the nodes to our platform.

## Can Tenzir see my data?

In theory *yes*, in practice *no*. Let us explain.

A Tenzir deployment consists of *nodes* that you manage, and a *platform*. The
*app* runs in your browser to access the platform. All computation and storage
takes place only at your nodes. The platform acts as rendezvous point that
connects two TLS-encrypted channels, one from the node to the platform, and one
from the browser to the platform:

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
[Sovereign Edition](https://tenzir.com/pricing) allows you to deploy the entire
platform in a dockerized environment, such as Docker Compose.

The [Professional Edition](https://tenzir.com/pricing) and [Enterprise
Edition](https://tenzir.com/pricing) are backed by a Tenzir-hosted instance of
the platform in the public cloud (AWS in Europe).

## Does Tenzir offer cloud-native nodes?

Tenzir currently does not offer cloud-hosted nodes. You can only run nodes in
your own environment, including your cloud environment.

However, we offer a cloud-native *demo* node that you can deploy as part of
every account.

## Why did you invent yet another query language? Why not SQL?

We opted for [our own language](pipelines.md) for several reasons that
we outline below.

At Tenzir, we have a clear target audience: security practitioners. They are
rarely also data engineers and fluent in SQL and experienced with lower-level
data tools. Rather, they identify as blue/purple teamers, incident responders,
threat hunters, detection engineers, threat intelligence analysts, and other
domain experts.

We opted for a dataflow language because it *easier* to comprehend: one step at
a time. At least conceptually, because a smart system optimizes the execution
under the hood. As long as the observable behavior remains the same, the
underlying implementation can optimize the actual computation at will. This is
especially noticeable with declarative languages, such as SQL, where the user
describes the *what* instead of the *how*. A dataflow language is a bit more
concrete in that it's closer to the *how*, but that's precisely the trade-off
that simplifies the reasoning: the focus is on a single operation at a time as
opposed to an entire large expression.

We spoke to countless security analysts that have decades of experience with
SIEMs. Splunk pioneered this space with their Search Processing Language (SPL).
Nowadays, Splunk gets a lot of flak for its pricing, but we admire how well it
caters to a broad audience of users that are *not* data engineers. SPL hit the
sweet spot of catering to its intended audience. Admittedly, SPL grew over time
and perhaps we'd start off differently today. To quote [Bjarne
Stroustrup](https://www.stroustrup.com/):

> There are only two kinds of languages: the ones people complain about and the
> ones nobody uses.

In fact, Splunk released SPL2 as an upgrade to SPL to make it more pipeline-ish.
And Elastic also doubled down as well with [ES|QL](/blog/a-first-look-at-esql).
Nearly every SIEM and adjacent observability tool has its own pipeline language.
When we designed our language, we had the fortune of starting with a clean slate
and could draw inspiration from others to achieve:

- the *familiarity* of [splunk](https://splunk.com)
- the *power* of [Kusto](https://github.com/microsoft/Kusto-Query-Language)
- the *expressiveness* of [dplyr](https://dplyr.tidyverse.org/)
- the *flexibility* of [jq](https://stedolan.github.io/jq/)
- the *ambition* of [Zed](https://zed.brimdata.io/)
- the *clarity* of [PRQL](https://prql-lang.org/)
- the *composability* of [Nu](https://www.nushell.sh/)

These may sound esoteric to some, but they are remarkably well designed
evolutions of pipeline languages that are not SQL. In fact, for a given dataflow
pipeline there's often an equivalent SQL expression, because the underlying
engines frequently map to the same execution model. This gives rise to
[transpiling dataflow languages to other execution
platforms][splunk-transpiler]. Ultimately, our goal is that security
practitioners do not have to think about *any* of this and stay in their happy
place, which means avoiding context switches to lower-level data primitives.

[splunk-transpiler]: https://www.databricks.com/blog/2022/12/16/accelerating-siem-migrations-spl-pyspark-transpiler.html

Our long-term strategy is to support as many language *frontends* as possible,
similar to Databricks building a [SIEM-to-Spark
transpiler](https://github.com/databrickslabs/transpiler). Emerging projects
like [substrait](https://substrait.io/) and [ibis](https://ibis-project.org/)
are first attempts. The [Composable Data Management System
Manifesto](https://dl.acm.org/doi/10.14778/3603581.3603604) puts these efforts
in a broader vision. As much as we support this vision, we cannot wait until
these fledgling projects reach production-grade quality, forcing us to walk the
fine line of rolling our and building on top existing technology, even if [it
is not always easy to
integrate](/blog/parquet-and-feather-data-engineering-woes).

Central to our effort is building an *open* system that's extensible. This is
why we leverage [Apache Arrow](https://arrow.apache.org), allowing anyone to
hook into a standardized data stream to deploy custom analytics. None of the
existing dataflow languages used by security people have this property. Neither
did we encounter the capability to write operators that work across multiple
schemas in a [rich type system](./data-model/type-system.md). We do not want
users to think of tables, but rather domain entities. But SQL puts tables front
and center. We'd rather put the concept of tables in the background because
security use cases often revolve around high-level concepts. When desired, it's
of course possible to restrict the analytics to a specific set of schemas, and
this is why our engine offers *multi-schema* dataflows. This is a novel approach
that fuses the ease of use from the world of document-oriented engines and
combines it with the power of engines that operate on semi-structured data.

Finally, we wanted to give users the ability to express both streaming and batch
processing workloads *in the same* language. In particular, we designed the
language so that switching between a historical query and live streaming is a matter of logically exchanging the pipeline input.

To support all of these ideas elegantly with a compelling user experience, we
needed a new language.

## What database does Tenzir use?

Tenzir does not rely on a third-party database.

Tenzir nodes include a light-weight storage engine on top of partitioned Feather
or Parquet files, accessible via the [`import`](operators/import.md) and
[`export`](operators/export.md) operators. The engine comes with a
catalog that tracks meta data and a thin layer of sketches to accelerate
queries.

Our [tuning guide](setup-guides/tune-performance/README.md) has further details
on the inner workings.

## Does a Tenzir node run on platform *X*?

We currently support the platforms that we mention in our [deployment
instructions](setup-guides/deploy-a-node.md).

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
