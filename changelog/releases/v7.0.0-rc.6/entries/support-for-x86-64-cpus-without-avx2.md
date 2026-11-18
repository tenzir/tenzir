---
title: Support for x86-64 CPUs without AVX2
type: bugfix
authors:
  - tobim
created: 2026-11-17T11:51:22.798207Z
---

Tenzir now runs on x86-64 CPUs that lack AVX2, such as Intel Ivy Bridge. Previously, the static Linux release and the container images aborted with an `Illegal instruction` error at startup on these machines.

Tenzir still requires SSE4.2, which Intel CPUs support since Nehalem (2008) and AMD CPUs since Bulldozer (2011). On CPUs that support AVX2, performance-critical code paths still detect and use it at runtime.
