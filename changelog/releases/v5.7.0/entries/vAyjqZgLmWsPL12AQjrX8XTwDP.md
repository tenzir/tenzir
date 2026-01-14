---
title: "Secrets"
type: feature
author: IyeOnline
created: 2025-12-17T09:23:13Z
pr: [5065,5197]
---

Tenzir now features a new first class type: `secret`. As the name suggests, this
type contains a secret value that cannot be accessed by a user:

```tql
from { s: secret("my-secret") }
```
```tql
{
  s: "***", // Does not render the secret value
}
```

A secret is created by the `secret` function, which changes its behavior with this
release.

Operators now accept secrets where appropriate, most notably for username and
password arguments, but also for URLs:

```tql
let $url = "https://" + secret("splunk-host") + ":8088"
to_splunk $url, hec_token=secret("splunk-hec-token")
```

However, a `string` is implicitly convertible to a `secret` in an operator
argument, meaning that you do not have to configure a secret if you are fine
with just a string literal:

```tql
to_splunk "https://localhost:8088", hec_token="my-plaintext-token"
```

Along with this feature in the Tenzir Node, we introduced secret stores to the
Tenzir Platform. You can now centrally manage secrets in the platform, which
will usable by all nodes within the workspace. Read more about this in the
release notes for the Tenzir Platform and our Explanations page on secrets.
