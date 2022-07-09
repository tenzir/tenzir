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
