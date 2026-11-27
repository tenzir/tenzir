---
title: Faster from_splunk on large result sets
type: bugfix
authors:
  - jachris
created: 2026-11-27T09:10:09.106376Z
---

The `from_splunk` operator now splits Splunk responses into results in time linear in the response size. Previously, a response chunk with many results took time quadratic in their number to process.
