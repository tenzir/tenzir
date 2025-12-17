---
title: "Accept numbers in place of strings in JSON"
type: bugfix
author: tobim
created: 2021-03-12T10:22:33Z
pr: 1439
---

The JSON parser now accepts data with numerical or boolean values in fields that
expect strings according to the schema. VAST converts these values into string
representations.
