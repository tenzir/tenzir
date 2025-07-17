---
title: "Add script to convert CIM to VAST taxonomy"
type: feature
authors: mavam
pr: 1121
---

The new script `splunk-to-vast` converts a splunk CIM model file in JSON to a
VAST taxonomy. For example, `splunk-to-vast < Network_Traffic.json` renders the
concept definitions for the *Network Traffic* datamodel. The generated taxonomy
does not include field definitions, which users should add separately according
to their data formats.
