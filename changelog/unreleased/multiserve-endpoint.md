---
title: Stateful multiserve REST endpoints
type: feature
authors:
  - lava
  - claude
created: 2026-06-24T00:00:00.000000Z
---

The new `/multiserve` REST endpoint family lets a client read from many pipeline
output streams through a single, server-managed session. Send `POST /multiserve`
without a `multiserve_id` to create a session seeded from a list of `serve_ids`;
the response returns the session's `multiserve_id` together with a `results`
object keyed by serve id, each carrying that stream's events, continuation
token, and state. Send `POST /multiserve` again with the `multiserve_id` to poll
the existing session for more events.

You can reshape a session's membership without recreating it: `POST
/multiserve/add` joins another output stream, `POST /multiserve/remove` drops
one, and `POST /multiserve/status` lists the current members along with their
continuation tokens and schema overrides. Requests that reference an unknown
`multiserve_id` return an error response.

Compared to the stateless `/serve-multi` endpoint, the session keeps track of
each member's continuation token for you, so a client no longer has to thread
per-stream tokens through every request.
