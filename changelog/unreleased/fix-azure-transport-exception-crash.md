---
title: Fix crash on Azure SSL/transport errors
type: bug
authors:
  - lava
created: 2026-03-24T00:00:00.000000Z
---

The Azure Blob Storage connector now handles `Azure::Core::Http::TransportException`
(e.g., SSL certificate errors) gracefully instead of crashing. Previously, a
self-signed certificate in the certificate chain would cause an unhandled
exception and terminate the node.
