---
title: Native Splunk semantics for operators after from_splunk
type: change
authors:
  - mavam
created: 2026-11-04T17:40:50.053002Z
---

`from_splunk` now hands Splunk the operators that follow it, as long as SPL can spell them: `where`, `select`, `drop`, `summarize`, `sort`, `head`, and `top` or `rare` followed by `head`. Those operators leave the pipeline and run in Splunk with Splunk's semantics:

```tql
from_splunk "https://splunk.example.com:8089",
  index="main",
  earliest="-1h",
  latest="now",
  headers={Authorization: secret("splunk-rest-authorization")}
where sourcetype == "linux_secure" and user != "root"
summarize user, attempts=count()
where attempts > 10
sort -attempts
head 20
```

- **Filters:** with `index`, filters right after the operator extend the base `search` command where it can spell them, such as `sourcetype == "linux_secure"`, so they match without regard to case, and networks match by CIDR. All other filters become `where` stages, which compare text exactly, drop events without the field for `!=`, and hold for a multivalue field if any of its values matches. Filters on `_time` also narrow `earliest` and `latest` when these are absolute.
- **Summaries:** `summarize` becomes `stats`, also for filters, sorts, and limits after it. Splunk drops events whose group key is `null`, puts an event with several values into the group of each value, and counts each value in `count(field)`. Counts are integers also with `raw=true`.
- **Sorts and limits:** `sort` keeps exactly as many events as the `head` after it, or all of them, rather than Splunk's default of 10,000.

Before, `from_splunk` only preselected events in Splunk and filtered them again in Tenzir, and it computed summaries in Splunk only where the result matched `summarize`, with a second search otherwise. It now sends a single search. A search that fails in a `stats` stage, which Splunk reports as an empty result, now fails the pipeline.

Use `pushdown=false` to keep every operator in the pipeline with TQL semantics. The search still returns only the fields that the pipeline needs:

```tql
from_splunk "https://splunk.example.com:8089",
  index="main",
  earliest="-1h",
  latest="now",
  headers={Authorization: secret("splunk-rest-authorization")},
  pushdown=false
where user != "root"
```

With `options`, or after a search with `fieldformat` or a macro, the operators stay in the pipeline. After a search with a command that has side effects, such as `outputlookup`, a `head` stays in the pipeline.
