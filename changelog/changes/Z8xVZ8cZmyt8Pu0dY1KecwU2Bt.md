---
title: "Add the `set` operator for upserting fields"
type: feature
authors: dominiklohmann
pr: 4057
---

The new `set` operator upserts fields, i.e., acts like `replace` for existing
fields and like `extend` for new fields. It also supports setting the schema
name explicitly via `set #schema="new-name"`.

The `put` operator now supports setting the schema name explicitly via `put
#schema="new-name"`.
