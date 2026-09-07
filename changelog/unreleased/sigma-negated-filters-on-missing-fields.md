---
title: Sigma filters on missing fields no longer suppress matches
type: bugfix
authors:
  - mavam
created: 2026-09-03T14:40:00Z
---

The `sigma` operator evaluated detection items in three-valued logic, so a
filter over a field the event does not carry produced `null`, and `not null`
stayed `null`. Rules using the common `selection and not filter_*` pattern
never matched events that lacked the filter's field. Items over missing fields
now evaluate to `false`, so negated filters pass as Sigma specifies.
