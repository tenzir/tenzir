---
title: VAST v1.0
description: VAST v1.0 – New Year, New Versioning Scheme
authors: dominiklohmann
date: 2022-01-27
last_updated: 2022-07-15
tags: [release, transforms, query]
---

We are happy to announce [VAST v1.0][github-vast-release]!

This release brings a new approach to software versioning for Tenzir. We laid
out the semantics in detail in a new [VERSIONING][github-versioning-md]
document.

[github-vast-release]: https://github.com/tenzir/vast/releases/tag/v1.0.0
[github-versioning-md]: https://github.com/tenzir/vast/blob/v1.0.0/VERSIONING.md

<!--truncate-->

## Query events based on their import time

The new [`#import_time` extractor][docs-meta-extractor] allows for exporting
events based on the time they arrived at VAST. Most of the time, this timestamp
is not far away from the timestamp of when the event occurred, but in certain
cases the two may deviate substantially, e.g., when ingesting historical events
from several years ago.

For example, to export all Suricata alerts that arrived at VAST on New Years Eve
as JSON, run this command:

```bash
vast export json '#type == "suricata.alert" && #import_time >= 2021-12-31 && #import_time < 2022-01-01'
```

This differs from the [`:timestamp` type extractor][docs-type-extractor] that
queries all events that contain a type `timestamp`, which is an alias for the
`time` type.  By convention, the `timestamp` type represents the event time
embedded in the data itself. However, the import time  is not part of the event
data itself, but rather part of metadata of every batch of events that VAST
creates.

[docs-meta-extractor]: https://vast.io/docs/VAST%20v3.0/understand/language/expressions#meta-extractor
[docs-type-extractor]: https://vast.io/docs/VAST%20v3.0/understand/language/expressions#type-extractor

## Omit `null` fields in the JSON export

VAST renders all fields defined in the schema when exporting events as JSON. A
common option for many tools that handle JSON is to skip rendering `null`
fields, and the new `--omit-nulls` option to the JSON export does exactly that.

To use it on a case-by-case basis, add this flag to any JSON export.

```bash
vast export json --omit-nulls '<query>'

# This also works when attaching to a matcher.
vast matcher attach json --omit-nulls <matcher>
```

To always enable it, add this to your `vast.yaml` configuration file:

```yaml
vast:
  import:
    omit-nulls: true
```

## Selection and Projection Transform Steps

:::info Transforms → Pipelines
In [VAST v2.2](/blog/vast-v2.2), we renamed *transforms* to *pipelines*, and
*transform steps* to *pipeline operators*. This caused several configuration key
changes. Please keep this in mind when reading the example below and consult the
[documentation](/docs/VAST%20v3.0/understand/language/pipelines) for the
up-to-date syntax.
:::

Reshaping data during import and export is a common use case that VAST now
supports. The two new built-in transform steps allow for filtering columns and
rows. Filtering columns (*projection*) takes a list of column names as input,
and filtering rows (*selection*)  works with an arbitrary query expression.

Here’s a usage example that sanitizes data leaving VAST during a query. If any
string field in an event contains the value `tenzir` or `secret-username`, VAST
will not include the event in the result set. The example below applies this
sanitization only to the events  `suricata.dns` and `suricata.http`, as defined
in the section `transform-triggers`.

```yaml
vast:
  # Specify and name our transforms, each of which are a list of configured
  # transform steps. Transform steps are plugins, enabling users to write more
  # complex transformations in native code using C++ and Apache Arrow.
  transforms:
     # Prevent events with certain strings to be exported, e.g., "tenzir" or
     # "secret-username".
     remove-events-with-secrets:
       - select:
           expression: ':string !in ["tenzir", "secret-username"]'

  # Specify whether to trigger each transform at server- or client-side, on
  # import or export, and restrict them to a list of event types.
  transform-triggers:
    export:
      # Apply the remove-events-with-secrets transformation server-side on
      # export to the suricata.dns and suricata.http event types.
      - transform: remove-events-with-secrets
        location: server
        events:
          - suricata.dns
          - suricata.http
```

## Threat Bus 2022.01.27

Thanks to a contribution from Sascha Steinbiss
([@satta](https://github.com/satta)), Threat Bus only reports failure when
transforming a sighting context if the return code of the transforming program
indicates failure.

A small peek behind the curtain: We’re building the next generation of Threat
Bus as part of VAST. We will continue to develop and maintain Threat Bus and its
apps for the time being.

Threat Bus 2022.01.27 is available [👉
here](https://github.com/tenzir/threatbus/releases/tag/2022.01.27).
