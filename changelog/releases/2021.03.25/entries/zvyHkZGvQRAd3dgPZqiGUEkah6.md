---
title: "Support nested records in the Arrow Builder"
type: feature
author: dominiklohmann
created: 2021-03-11T10:26:24Z
pr: 1429
---

VAST now supports nested records in Arrow table slices and in the JSON import,
e.g., data of type `list<record<name: string, age: count>`. While nested record
fields are not yet queryable, ingesting such data will no longer cause VAST to
crash. MessagePack table slices don't support records in lists yet.
