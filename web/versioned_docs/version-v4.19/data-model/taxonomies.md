---
sidebar_position: 3
---

# Taxonomies

Event taxonomies address the [uphill battle][chuvakin19] of data normalization.
They enable you to interact with different data formats with a unified access
layer, instead of having to juggle the various naming schemes and
representations of each individual data source. Today, every SIEM has its own
"unified" approach to represent data, e.g.,
elastic's [ECS][ecs],
splunk's [CIM][splunk-cim],
QRadar's [LEEF][leef],
Sentinel's [ASIM][asim],
Chronicle's [UDM][udm-chronicle],
Panther's [UDM][udm-panther],
and the XDR Alliance's [CIM][xdr-cim]
There exist also vendor-agnostic with a varying focus, such as MITRE's
[CEE][cee], OSSEM's [CDM][ossem], or STIX [SCOs][stix-scos].
Several vendors joined forces and launched the [Open Cybersecurity Schema
Framework (OCSF)][ocsf], an open and extensible project to create a [universal
schema][ocsf-schema].

[chuvakin19]: https://medium.com/anton-on-security/security-correlation-then-and-now-a-sad-truth-about-siem-fc5a1afb1001
[cee]: https://cee.mitre.org/
[ecs]: https://www.elastic.co/guide/en/ecs/current/ecs-reference.html
[splunk-cim]: https://docs.splunk.com/Splexicon:CommonInformationModel
[leef]: https://www.ibm.com/docs/en/dsm?topic=leef-overview:
[asim]: https://docs.microsoft.com/en-us/azure/sentinel/normalization
[udm-chronicle]: https://cloud.google.com/chronicle/docs/unified-data-model/udm-usage
[udm-panther]: https://docs.panther.com/writing-detections/data-models
[ossem]: https://github.com/OTRF/OSSEM-CDM
[stix-scos]: https://docs.oasis-open.org/cti/stix/v2.1/os/stix-v2.1-os.html#_mlbmudhl16lr
[xdr-cim]: https://github.com/XDR-Alliance/Common-Information-Model
[ocsf]: https://ocsf.io
[ocsf-schema]: https://schema.ocsf.io

We could add [yet another data model](https://xkcd.com/927/), but our goal is
that you pick one that you know already or like best. We envision a thriving
community around taxonomization, as exemplified with the [OCSF][ocsf]. With
Tenzir, we aim for leveraging the taxonomy of your choice.

:::info Concepts
A [concept](#concepts) is a field mapping/alias that lazily resolves at query
time.
:::

Concepts are not embedded in the schema and can therefore evolve independently
from the data typing. This behavior is different from other systems that
normalize by *rewriting* the data on ingest, e.g., elastic with [ECS][ecs]. We
do not advocate for this approach, because it has the following drawbacks:

- **Data Lock-in**: if you want to use a different data model tomorrow, you
  would have to rewrite all your past data, which can be infeasible in some
  cases.
- **Compliance Problems**: if you need an exact representation of your original
  data shape, you cannot perform an irreversible transformation.
- **Limited Analytics**: if you want to run a tool that relies on the original
  schema of the data, it will not work.

[Type aliases](type-system) and concepts are two different mechanisms to add
semantics to the data. The following table highlights the differences between
the two mechanisms:

|                  | Aliases                  | Concepts
|------------------|--------------------------|--------------------
| Objective        | Tune data representation | Model a domain
| User             | Schema writer            | Query writer
| Typing           | Strong                   | Lazy
| Location         | Embedded in data         | Defined outside of data
| Modification     | Only for new data        | For past and new data
| Structure        | Type hierarchy           | Tag-like collection

:::caution The Imperfection of Data Models
Creating a unified data model is conceptually The Right Thing, but prior to
embarking on a long journey, we have to appreciate that it will always remain an
imperfect approximation in practice, for the following reasons:

- **Incompleteness**: we have to appreciate that all data models are incomplete
  because data sources continuously evolve.
- **Incorrectness**: in addition to lacking information, data models contain
  a growing number of errors, for the same evolutionary reasons as above.
- **Variance**: data models vary substantially between products, making it
  difficult to mix-and-match semantics.
:::

## Concepts

A *concept* is a set of [extractors][extractors] to enable more semantic
querying. Tenzir translates a query expression containing a concept to a
disjunction of all extractors.

[extractors]: ../language/expressions.md#extractors

For example, Consider Sysmon and Suricata events, each of which have a notion of
a network connection with a source IP address. The Sysmon event
`NetworkConnection` contains a field `SourceIp` and the Suricata event `flow`
contains a field `src_ip` for this purpose. Without concepts, querying for a
specific value would involve writing a disjunction of two predicates:

```c
suricata.flow.src_ip == 6.6.6.6 || sysmon.NetworkConnection.SourceIp == 6.6.6.6
```

With concepts, you can write this as:

```c
source_ip == 6.6.6.6
```

Concepts decouple semantics from syntax and allow you to write queries that
"scale" independent of the number of data sources. No one wants to remember
all format-specific names, aside from being an error-prone practice.

You can define a concept in a [module](modules) as follows:

```yaml
concepts:
  source_ip:
    description: the originator of a network-layer connection
    fields:
    - sysmon.NetworkConnection.SourceIp
    - suricata.flow.src_ip
```

Concepts compose. A concept can include other concepts to represent semantic
hierarchies. For example, consider our above `source_ip` concept. If we want to
generalize this concept to also include MAC addresses, we could define a concept
`source` that includes both `source_ip` and a new field that represents a MAC
address:

![Concept Composition](concept-composition.excalidraw.svg)

You define the composite concept in a module as follows:

```yaml
concepts:
  source_ip:
    description: the originator of a connection
    fields:
    - zeek.conn.id.orig_l2_addr
    concepts:
    - source_ip
```

You can add new mappings to an existing concept in every module. For example,
when adding a new data source that contains an event with a source IP address
field, you can define the concept in the corresponding module.
