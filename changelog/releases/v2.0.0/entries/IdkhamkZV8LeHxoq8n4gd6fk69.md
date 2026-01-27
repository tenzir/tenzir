---
title: "Lift selector field requirements for JSON import"
type: bugfix
author: dominiklohmann
created: 2022-05-02T19:16:40Z
pr: 2255
---

The JSON import no longer rejects non-string selector fields. Instead, it always
uses the textual JSON representation as a selector. E.g., the JSON object
`{id:1,...}` imported via `vast import json --selector=id:mymodule` now matches
the schema named `mymodule.1` rather than erroring because the `id` field is not
a string.
