---
title: "Support for duplicate keys in parsers"
type: feature
author: IyeOnline
created: 2025-12-08T16:56:43Z
pr: 5445
---

Our parsers now have improved support for repeated keys in a an event.
Previously a later key-value pair would always overwrite the previous one.
With this change the value is transparently upgraded to a list of values.
