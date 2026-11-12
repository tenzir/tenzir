---
title: Imported events keep the exact registered schema
type: bugfix
authors:
  - IyeOnline
created: 2026-11-10T13:00:00Z
---

The `import` operator now keeps hidden attributes from the old data model on
events with a registered schema, such as `suricata.flow`. Before, these events
were stored under a second schema next to the one from older versions.
