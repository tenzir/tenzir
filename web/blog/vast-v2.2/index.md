---
title: VAST v2.2
description: Pipelines
authors: lava
date: 2022-08-05
tags: [release, summarize, pipelines]
---

We released [VAST v2.2][github-vast-release] 🙌! Transforms now have a new name:
[pipelines](/blog/vast-v2.2#transforms-are-now-pipelines). The [summarize
operator](/blog/vast-v2.2#summarization-improvements) also underwent a facelift,
making aggregation functions pluggable and allowing for assigning names to
output fields.

[github-vast-release]: https://github.com/tenzir/vast/releases/tag/v2.2.0

<!--truncate-->

## Transforms are now Pipelines

After carefully reconsidering our naming decisions related to query execution
and data transformation, we came up with a naming convention that does a better
job in capturing the underlying concepts.

Most notably, we renamed *transforms* to *pipelines*. A transform *step* is now a
pipeline *operator*. This nomenclature is much more familiar to users coming
from dataflow and collection-based query engines. The implementation underneath
hasn't changed. As in the [Volcano model][volcano], data still flows through
operators, each of which consumes input from upstream operators and produces
output for downstream operators. What we term a pipeline is the sequence of such
chained operators.

[volcano]: https://paperhub.s3.amazonaws.com/dace52a42c07f7f8348b08dc2b186061.pdf

While pipelines are not yet available at the query layer, they soon will be.
Until then, you can deploy pipelines at load-time to [transform data in motion
or data at rest](/docs/VAST%20v3.0/use/transform).

From a user perspective, the configuration keys associated with transforms have
changed. Here's the updated example from our previous [VAST v1.0 release
blog](/blog/vast-v1.0).

```yaml
vast:
  # Specify and name our pipelines, each of which are a list of configured
  # pipeline operators. Pipeline operators are plugins, enabling users to 
  # write complex transformations in native code using C++ and Apache Arrow.
  pipelines:
     # Prevent events with certain strings to be exported, e.g., 
     # "tenzir" or "secret-username".
     remove-events-with-secrets:
       - select:
           expression: ':string !in ["tenzir", "secret-username"]'

  # Specify whether to trigger each pipeline at server- or client-side, on
  # `import` or `export`, and restrict them to a list of event types.
  pipeline-triggers:
    export:
      # Apply the remove-events-with-secrets transformation server-side on
      # export to the suricata.dns and suricata.http event types.
      - pipeline: remove-events-with-secrets
        location: server
        events:
          - suricata.dns
          - suricata.http
```

## Summarization Improvements

In line with the above nomenclature changes, we've improved the behavior of the
[`summarize`][summarize] operator. It is now possible to specify an explicit
name for the output fields. This is helpful when the downstream processing needs
a predictable schema. Previously, VAST took simply the name of the input field.
The syntax was as follows:

```yaml
summarize:
  group-by:
    - ...
  aggregate:
    min:
      - ts # implied name for aggregate field
```

We now switched the syntax such that the new field name is at the beginning:

```yaml
summarize:
  group-by:
    - ...
  aggregate:
    ts_min: # explicit name for aggregate field
      min: ts
```

In SQL, this would be the `AS` token: `SELECT min(ts) AS min_ts`.

[summarize]: /docs/VAST%20v3.0/understand/language/operators/summarize
