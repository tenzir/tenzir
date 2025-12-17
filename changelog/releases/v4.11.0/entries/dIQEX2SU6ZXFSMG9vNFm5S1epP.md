---
title: "Introduce `--replace`, `--separate`, and `--yield` for contexts"
type: feature
author: dominiklohmann
created: 2024-03-15T18:25:37Z
pr: 4040
---

The `--replace` option for the `enrich` operator causes the input values to be
replaced with their context instead of extending the event with the context,
resulting in a leaner output.

The `--separate` option makes the `enrich` and `lookup` operators handle each
field individually, duplicating the event for each relevant field, and
returning at most one context per output event.

The `--yield <field>` option allows for adding only a part of a context with the
`enrich` and `lookup` operators. For example, with a `geoip` context with a
MaxMind country database, `--yield registered_country.iso_code` will cause the
enrichment to only consist of the country's ISO code.
