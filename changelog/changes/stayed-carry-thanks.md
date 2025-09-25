---
title: "Support duplicate keys in parsers"
type: feature
authors: IyeOnline
pr: 5445
---

Our parsers now have improved support for duplicate/repeated keys in a an event.
Previously a later key-value pair would always overwrite the previous one.
With this change the value is transparently upgraded to a list of values.
