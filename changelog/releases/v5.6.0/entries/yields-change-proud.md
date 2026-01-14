---
title: "HTTP request metadata"
type: feature
author: raxyte
created: 2025-06-24T09:04:50Z
pr: 5295
---

The `from_http` operator now supports the `metadata_field` option when using the
server mode and not just client mode. The request metadata has the following
schema:

| Field                | Type     | Description                           |
| :------------------- | :------- | :------------------------------------ |
| `headers`            | `record` | The request headers.                 |
| `query`              | `record` | The query parameters of the request.  |
| `path`               | `string` | The path requested.                   |
| `fragment`           | `string` | The URI fragment of the request.      |
| `method`             | `string` | The HTTP method of the request.       |
| `version`            | `string` | The HTTP version of the request.      |
