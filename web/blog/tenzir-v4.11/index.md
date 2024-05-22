---
title: Tenzir v4.11
authors: [dominiklohmann]
date: 2024-03-22
tags: [release, contexts, every, set, email, sqs]
comments: true
---

Our latest [v4.11](https://github.com/tenzir/tenzir/releases/tag/v4.11.1)
release delivers powerful automation features, such as scheduling pipelines in a
given time interval and sending pipeline data as emails.

![Tenzir v4.11](tenzir-v4.11.excalidraw.svg)

<!-- truncate -->

## Execute Sources on a Schedule

One feedback we've heard often from users is that the
[`from <url>`](/connectors) invocation is indeed handy, but it's more practical
when it's not a one-time gig. Users expressed a need to retrieve data from
various sources more than just once, indicating a requirement for a more
cyclical or scheduled approach.

Given these requirements, we initially considered adding options for continuous
data retrieval or polling for specific connectors, such as
[`http`](/connectors/http). However, we realized that the need for such
functionality ranged beyond a limited number of connectors. Hence, any solution
we developed would ideally adapt to any source operator, providing wider
functionality.

In response to these needs, we developed a new [operator
modifier](/next/language/operator-modifiers) that empowers any source operator
to execute at regular intervals: `every <interval>`.

For instance, the operator `every 1s from <url>` will enable the system to poll
the specified URL every single second. The capability delivers continuous,
real-time data access, considerably improving the feasibility and efficiency of
tasks requiring frequent data updates.

One area where we've found the `every <interval>` modifier to be especially
valuable is in the context of updating contexts. Consider a pipeline designed to
update a lookup-table context titled `threatfox-domains` once every hour. This
operation, which fetches IOCs (Indicators of Compromise) from the ThreatFox API,
can be achieved using the following pipeline:

```
every 1 hour from https://threatfox-api.abuse.ch/api/v1/ query=get_iocs days:=1
| yield data[]
| where ioc_type == "domain"
| context update threatfox-domains --key ioc
```

This pipeline initiates a query to retrieve the IOCs for the day from the
ThreatFox API. The pipeline subsequently filters out the data relevant to
domains and updates the `threatfox-domains` context. The entire pipeline
refreshes every hour as specified by the `every 1 hour` operator modifier.

The `every <interval>` operator modifier thus adds a powerful tool to our
arsenal, increasing our capabilities by adapting to any source operator for
scheduled execution.

## Enrich More Flexibly

A customer of ours asked a seemingly simple question: If I have a lookup-table
context that contains entries in the form `{"key": "DE", "context": {"flag":
"🇩🇪"}}` and want to use it to replace country short codes with their respective
flag as an emoji, how can I do that?

If you're just replacing the value of a single field then it's easy—you can just
use [`put`](/operators/put) to replace the input value with its context after
the enrichment. But this user wanted to look into every single string in every
event, and replace all country short codes that it contained.

Two newly added options for the [`enrich`](/next/operators/enrich) operator make
this easily possible:

```
…
| enrich country-flags --field :string --yield flag --replace
```

The `--replace` flag causes `enrich` to replace fields with their context, if
they exists. The option `--yield <field>` trims down the enrichment to just a
specific field within the context. The `--yield` option is also available for
the [`lookup`](/operators/lookup) operator.

```json title="Before"
{
  "source_ip": 212.12.56.176,
  "source_iso_code": "DE",
  "dest_ip": 8.8.8.8,
  "dest_iso_code": "US"
}
```

```json title="After"
{
  "source_ip": 212.12.56.176,
  "source_iso_code": "🇩🇪",
  "dest_ip": 8.8.8.8,
  "dest_iso_code": "🇺🇸"
}
```

The other new option of `lookup` and `enrich` is `--separate`, which creates
separate events for every enrichment. This causes events to be duplicated for
every enrichment from a context that applies, with one enrichment per event in
the result. This is particularly useful in `lookup` when evaluating a large set
of IOCs to create separate alerts per IOC even within a single event.

```json title="Enriched as one event"
{
  "source_ip": 212.12.56.176,
  "source_iso_code": "DE",
  "dest_ip": 8.8.8.8,
  "dest_iso_code": "US",
  "flags": {
    "source_ip": {
      "value": "212.12.56.176"
      "timestamp": "2024-03-21T15:12:07.493155",
      "mode": "enrich",
      "context": {
        "flag": "🇩🇪"
      }
    }
    "dest_ip": {
      "value": "8.8.8.8"
      "timestamp": "2024-03-21T15:12:07.493155",
      "mode": "enrich",
      "context": {
        "flag": "🇺🇸"
      }
    }
  }
}
```

```json title="Enriched as separate events"
{
  "source_ip": 212.12.56.176,
  "source_iso_code": "DE",
  "dest_ip": 8.8.8.8,
  "dest_iso_code": "US",
  "flags": {
    "path": "source_ip",
    "value": "212.12.56.176"
    "timestamp": "2024-03-21T15:12:07.493155",
    "mode": "enrich",
    "context": {
      "flag": "🇩🇪"
    }
  }
}
{
  "source_ip": 212.12.56.176,
  "source_iso_code": "DE",
  "dest_ip": 8.8.8.8,
  "dest_iso_code": "US",
  "flags": {
    "path": "source_ip",
    "value": "8.8.8.8"
    "timestamp": "2024-03-21T15:12:07.493155",
    "mode": "enrich",
    "context": {
      "flag": "🇺🇸"
    }
  }
}
```

## The Sweet Spot Between Extend and Replace

The [`set`](/next/operators/set) operator "upserts" into events. Its syntax
exactly matches the syntax of the existing [`extend`](/next/operators/extend),
[`replace`](/next/operators/replace), and [`put`](/next/operators/put)
operators.

If a specified field already exists, the `set` operator replaces its value. If
it does not, the `set` operator extends the event with new field. We found this
behavior to be quite intuitive, and in most cases we now reach for `set` instead
of `replace` and `extend`.

:::tip Setting the Schema Name
The `set`, `put`, and `replace` operator support changing the schema name of
events. For example, `set #schema="foo.bar"` will show up as a schema `bar` in
the category `foo` in the Explorer on [app.tenzir.com](https://app.tenzir.com).
:::

## Send Emails from a Pipeline

The new [`email`](/next/connectors/email) saver sends away pipeline contents as
mails. This is especially handy for integrating with traditional escalation
pathways that rely on email-based dispatching methods.

For example, to send all Suricata alerts arriving at a node via email, use:

```
export --live
| where #schema == "suricata.alert"
| write json
| save email alerts@example.org --from "tenzir@example.org" --subject Alert
```

The `email` saver supports both SMTP and SMTPS. The default endpoint is
`smtp://localhost:25`, but you can provide any other server. Instead of copying
the rendered JSON directly into the email body, you can also provide the
`--mime` to send a MIME-encoded chunk that uses the MIME type according to the
format you provided.

## Working with Amazon SQS Queues

The new [`sqs`](/next/connectors/sqs) enables reading from and writing to Amazon
SQS queues. For example, importing JSON from an SQS queue named `tenzir` into a
node looks like this:

```
from sqs://tenzir | import
```

## Other Changes

As usual, the complete list of bug fixes, adjustments, and enhancements
delivered with this version can be found in [the changelog](/changelog#v4110).

Explore the latest features at [app.tenzir.com](https://app.tenzir.com) and
connect with us on [our Discord server](/discord).
