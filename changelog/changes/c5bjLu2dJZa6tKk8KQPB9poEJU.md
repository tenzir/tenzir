---
title: "Fix overzealous parameter validation in `/pipeline/launch`"
type: bugfix
authors: dominiklohmann
pr: 4919
---

We fixed an overzealous parameter validation bug that prevented the use of the
`/pipeline/launch` API endpoint when specifying a `cache_id` without a
`serve_id` when `definition` contained a definition for a pipeline without a
sink.
