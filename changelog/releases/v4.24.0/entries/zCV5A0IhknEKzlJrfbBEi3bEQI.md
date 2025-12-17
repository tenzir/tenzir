---
title: "Port `unordered`, `local`, and `remote` to TQL2"
type: feature
author: dominiklohmann
created: 2024-12-03T13:56:14Z
pr: 4835
---

The `local` and `remote` operators allow for overriding the location of a
pipeline. Local operators prefer running at a client `tenzir` process, and
remote operators prefer running at a remote `tenzir-node` process. These
operators are primarily intended for testing purposes.

The `unordered` operator throws away the order of events in a pipeline. This
causes some operators to run faster, e.g., `read_ndjson` is able to parse events
out of order through this. This operator is primarily intended for testing
purposes, as most of the time the ordering requirements are inferred from
subsequent operators in the pipeline.
