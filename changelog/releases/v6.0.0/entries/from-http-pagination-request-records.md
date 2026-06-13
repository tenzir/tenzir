---
title: Request records for `from_http` pagination
type: feature
authors:
  - mavam
  - codex
prs:
  - 6193
created: 2026-05-17T13:45:00Z
---

The `from_http` operator now supports returning request records from
`paginate` lambdas. This lets APIs keep pagination state in the next request
body or headers instead of only in the next URL:

```tql
from_http "https://opensearch.example.com/logs/_search",
  method="post",
  body={size: 500, query: {match_all: {}}},
  paginate=(x => {
    body: {
      size: 500,
      query: {match_all: {}},
      search_after: x.hits.hits[-1].sort,
    },
  } if x.hits.hits != []) {
  read_json
}
```

Returned request records can patch `url`, `method`, `headers`, and `body`.
Missing fields inherit from the current request, and `body: null` clears the
body.
