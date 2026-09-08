---
title: Source locations in node startup diagnostics
type: change
authors:
  - aljazerzen
  - claude
created: 2026-08-27T10:39:53.247829Z
---

Diagnostics that a node emits for configured operators and for pipelines
that ship with a package now name the file or pipeline they come from.

Previously a broken `tenzir.operators` definition or package pipeline
produced only the bare message:

```
error: operator `wherex` not found
  = hint: did you mean `where`?
```

Now the offending definition is pointed at directly, both on the console
and in the node log:

```
error: operator `wherex` not found
 --> tenzir.operators.myop:1:10
  |
1 | head 5 | wherex true
  |          ^^^^^^
  = hint: did you mean `where`?
```

Errors from packages that fail to load also appear in the node log with
their source location and notes instead of an unstructured dump.

Diagnostics whose location lies inside a user-defined operator that the
pipeline expanded now point into the operator's own definition and name the
invocation as the call site, instead of at an unrelated span of the pipeline
that used it:

```
error: operator `nonexistent` not found
 --> /etc/tenzir/packages/acme/operators/mark.tql:1:1
  |
1 | nonexistent
  | ^^^^^^^^^^^
  |
 --> <packages/acme/enrich>:2:1
  |
2 | acme::mark
  | ^^^^^^^^^^ called from here
```
