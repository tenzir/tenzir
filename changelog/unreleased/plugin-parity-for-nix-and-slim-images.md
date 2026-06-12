---
title: Plugin parity for Nix and slim images
type: change
authors:
  - aljazerzen
  - claude
created: 2026-06-12T12:47:50.563954Z
---

The Nix-built Tenzir packages and `-slim` container images now include the
same proprietary plugins as the main container images, including
`from_sentinelone_data_lake`, `to_sentinelone_data_lake`, and the Snowflake
connector.

This means workflows that depend on these plugins now work the same way across
both the regular and `-slim` image variants.
