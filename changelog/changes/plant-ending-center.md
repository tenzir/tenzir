---
title: "Versioned sources in `to_amazon_security_lake` operator"
type: feature
authors: IyeOnline
pr: 5369
---

The `to_amazon_security_lake` operator now supports versioned custom sources,
such as

```tql
let $lake_url = "s3://aws-security-data-lake-eu-west-2-lake-abcdefghijklmnopqrstuvwxyz1234/ext/tnz-ocsf-dns/1.0/"
to_amazon_security_lake $lake_url, …
```
