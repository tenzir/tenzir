---
title: Tenzir v4.11
authors: [dominiklohmann]
date: 2024-03-20
tags: [contexts, every, email, sqs]
comments: true
---

TODO Matthias intro text + replace the image

[Tenzir v4.11](https://github.com/tenzir/tenzir/releases/tag/v4.11.0) ...

<!-- ![Tenzir v4.11](tenzir-v4.11.excalidraw.svg) -->

<!-- truncate -->

## Execute Sources on a Schedule

One feedback we've heard often from users is that the `from <url>` function is
indeed handy, but it's more practical when it's not a one-time gig. Users
expressed a need to retrieve data from various sources more than just once,
indicating a requirement for a more cyclical or scheduled approach.

Given these requirements, we initially considered adding options for continuous
data retrieval or polling for specific connectors, such as `http`. However, we
realized that the need for such functionality ranged beyond a limited number of
connectors. Hence, any solution we developed would ideally adapt to any source
operator, providing wider functionality.

In response to these needs, we developed a new operator modifier, that empowers
any source operator to execute at regular intervals: `every <interval>`. 

For instance, the operator `every 1s from <url>` will enable the system to poll
the specified URL every single second. The capability delivers continuous,
real-time data access, considerably improving the feasibility and efficiency of
tasks requiring frequent data updates.

One area where we've found the `every <interval>` modifier to be especially
valuable is in the context of updating contexts. For instance, consider a
pipeline designed to update a lookup-table context titled `threatfox-domains`
once every hour. This operation, which fetches IOCs (Indicators of Compromise)
from the ThreatFox API, can be achieved using the following source code:

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
use `put` to replace the input value with its context after the enrichment. But
this user wanted to look into every single string in every event, and replace
all country short codes that it contained.

Two newly added options for the `enrich` operator make this easily possible:

```
…
| enrich country-flags --field :string --yield flag --replace
```

The `--replace` flag causes the `enrich` operator to replace fields with their
context, if it exists. `--yield <field>` trims down the enrichment to just a
specific field within the context. The `--yield` option is also available for
the `lookup` operator.

The `lookup` and `enrich` operator gained a new option to create separate events
for every enrichment with the `--separate` flag. This causes events to be
duplicated for every enrichment from a context that applies, with one enrichment
per event in the result. This is particularly useful in `lookup` when evaluating
a large set of IOCs to create separate alerts per IOC even within a single
event.

## Integrate with SQS

TODO Matthias

## Send Emails from a Pipeline

TODO Matthias

## Other Changes

As usual, the complete list of bug fixes, adjustments, and enhancements
delivered with this version can be found in [the changelog](/changelog#v4110).

Explore the latest features at [app.tenzir.com](https://app.tenzir.com) and
connect with us on [our Discord server](/discord).
