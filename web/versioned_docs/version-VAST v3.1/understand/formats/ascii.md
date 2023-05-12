---
description: Typed textual data
---

# ASCII

The `ascii` format renders data according to VAST's [value
syntax][expression-values]. A value is a data literal that makes easy to infer
its type. The main value is that it represents a maximally condensend view of
heterogeneous data. Think of it like NDJSON, but without field names.

[expression-values]: ../expressions.md#values

For example, `1.2.3.4` is an IP address literal, whereas `"1.2.3.4"`
is a string literals. Type inference isn't always possible, e.g., in the case of
empty lists (`[]`).

## Printer

Use `export ascii` to render data in textual form:

```bash
vast export ascii '#type == /.*/'
```

```
<2011-08-14T05:38:53.914038, 929669869939483, null, null, null, 147.32.84.165, 138, 147.32.84.255, 138, "UDP", "flow", null, 2, 0, 486, 0, 2011-08-12T12:53:47.928539, 2011-08-12T12:53:47.928552, 0, "new", "timeout", F, "failed">
<2011-08-12T13:00:36.378914, 269421754201300, 22569, null, null, 147.32.84.165, 1027, 74.125.232.202, 80, "TCP", "http", null, "cr-tools.clients.google.com", "/service/check2?appid=%7B430FD4D0-B729-4F61-AA34-91526481799D%7D&appversion=1.3.21.65&applang=&machine=0&version=1.3.21.65&osversion=5.1&servicepack=Service%20Pack%202", null, "Google Update/1.3.21.65;winhttp", null, "GET", null, "HTTP/1.1", null, null, 0, 0>
```
