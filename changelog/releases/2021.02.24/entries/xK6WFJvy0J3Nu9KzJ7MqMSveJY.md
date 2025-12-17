---
title: "Make it easier to reference user defined types in the schema language"
type: change
author: tobim
created: 2021-02-15T07:55:46Z
pr: 1331
---

Schema parsing now uses a 2-pass loading phase so that type aliases can
reference other types that are later defined in the same directory.
Additionally, type definitions from already parsed schema dirs can be referenced
from schema types that are parsed later. Types can also be redefined in later
directories, but a type can not be defined twice in the same directory.
