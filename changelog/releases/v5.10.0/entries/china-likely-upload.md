---
title: "Operations on concatenated secrets"
type: feature
author: IyeOnline
created: 2025-07-11T19:13:16Z
pr: 5324
---

You can now arbitrarily nest operations on secrets. This is useful for APIs that
expect authentication is an encoded blob:

```tql
let $headers = {
  auth: f"{secret("user")}:{secret("password")}".encode_base64()
}
```
