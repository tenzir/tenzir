---
draft: true
title: VAST v2.2
description: VAST v2.2 - TBD
authors: dominiklohmann
date: 2022-08-07
tags: [release, summarize]
---

We released [VAST v2.2][github-vast-release] 🙌!

[github-vast-release]: https://github.com/tenzir/vast/releases/tag/v2.2.0

<!--truncate-->

## Summarization Improvements

We've improved the behavior of the [`summarize`][summarize] operator. It is now
possible to specify an explicit name for the output fields. This is helpful when
the downstream processing needs a predictable schema. Previously, VAST took
simply the name of the input field. The syntax was as follows:

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

[summarize]: /docs/understand-vast/query-language/operators/summarize

## Transform is now Pipeline

We reconsidered our naming decisions of the various concepts related to query
execution and data transformation, and came up with a naming convention that we
believe does a better job in capturing the underlying concepts. A data
transformation is now represented as a _pipeline_ which is itself a sequence of
_pipeline operators_ data flows through while being transformed into the
desired form.

The associated configuration keys have changed. Here's the updated example from
a previous [blog][blog1.0] announcing VAST v1.0:

[blog1.0]: http://localhost:3000/blog/vast-v1.0#selection-and-projection-transform-steps

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
  # import or export, and restrict them to a list of event types.
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

