---
title: "Fix `serve` exiting prematurely"
type: bugfix
author: dominiklohmann
created: 2023-10-19T13:45:06Z
pr: 3562
---

Pipelines ending with the `serve` operator no longer incorrectly exit 60 seconds
after transferring all events to the `/serve` endpoint, but rather wait until
all events were fetched from the endpoint.

Shutting down a node immediately after starting it now no longer waits for all
partitions to be loaded.
