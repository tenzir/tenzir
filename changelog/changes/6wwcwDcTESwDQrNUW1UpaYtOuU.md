---
title: "Add new loading mechanism for GeoIP context"
type: feature
authors: balavinaithirthan
pr: 4158
---

The `geoip` context now supports loading in a MaxMind database with `context
load <ctx>`. For example, `load s3://my-bucket/file.mmdb | context load my-ctx`
makes the GeoIP context use a remotely stored database.
