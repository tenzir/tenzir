---
title: "Fixed `to_amazon_security_lake` partitioning"
type: bugfix
author: IyeOnline
created: 2025-07-28T12:38:17Z
pr: 5369
---

The `to_amazon_security_lake` incorrectly partitioned as `…/accountID=…`. It now
uses the correct `…/accountId=…`.
