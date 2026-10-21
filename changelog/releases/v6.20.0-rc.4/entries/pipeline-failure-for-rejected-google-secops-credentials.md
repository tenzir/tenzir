---
title: Pipeline failure for rejected Google SecOps credentials
type: bugfix
authors:
  - mavam
created: 2026-10-13T08:08:05.248353Z
---

The `to_google_secops` operator now fails the pipeline when Google SecOps rejects a request with HTTP 401 (Unauthorized) or 403 (Forbidden). Previously, the operator emitted a warning, discarded the batch, and kept going. Invalid credentials or missing permissions could therefore leave a pipeline running, or let it complete successfully, without delivering any events.

The error includes the response from Google SecOps and a hint to check the credentials or their permissions for the instance.
