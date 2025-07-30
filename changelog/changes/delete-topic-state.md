---
title: "Fixed `to_amazon_security_lake` partitioning"
type: bugfix
authors: IyeOnline
pr: 5369
---

The `to_amazon_security_lake` incorrectly partitioned as `…/accountID=…`. It now
uses the correct `…/accountId=…`.
