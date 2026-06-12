---
title: Selector defaults for UDO field parameters
type: feature
authors:
  - mavam
  - claude
prs:
  - 6352
created: 2026-06-12T15:07:30.532323Z
---

Field-typed parameters of user-defined operators now accept selectors as defaults in the TQL frontmatter, such as `this`, `this.name`, or `foo.bar`. Previously, the only allowed default for a `field` parameter was `null`.

For example, this operator wraps a field into a record and operates on the entire event when no argument is given:

```tql
---
args:
  named:
    - name: field
      type: field
      default: this
---
this = {wrapped: $field}
```

Calling the operator without arguments wraps the whole event, while passing `field=name` wraps just that field. This makes it easy to write mapping operators that work on the full event by default but can be scoped to a subrecord on demand.
