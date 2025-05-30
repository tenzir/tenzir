---
title: "Implement context backends for the contextualizer"
type: feature
authors: Dakostu
pr: 3684
---

The closed-source `context` plugin offers a backend functionality for
finding matches between data sets.

The new `lookup-table` built-in is a hashtable-based
contextualization algorithm that enriches events based on a unique value.

The JSON format has a new `--arrays-of-objects` parameter that allows for
parsing a JSON array of JSON objects into an event for each object.
