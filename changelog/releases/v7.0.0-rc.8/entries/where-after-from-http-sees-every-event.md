---
title: A `where` after `from_http` sees every event
type: bugfix
authors:
  - mavam
  - IyeOnline
created: 2026-11-24T12:00:00.000000Z
---

A `where` after `from_http` now also applies to the events that `error_field`
produces. Previously, these events passed the filter unchecked.

Lambda and OData pagination no longer stop early when a downstream `where`
drops the events of a page. Previously, `from_http` stopped fetching further
pages because the filtered page appeared to have no events to paginate from.
