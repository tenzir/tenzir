---
title: "Secrets"
type: feature
authors: IyeOnline
pr: [5065,5197]
---

Tenzir now features a new first class type: `secret`. As the name suggests, this
type contains a secret value that cannot accessed by a user:

```tql
from { s: secret("my-secret") }
```
```tql
{
  s: "***", // Does not render the secret value
}
```

A secret is created by the `secret` function, which changes its behavior with this
release. If you want to retain the old behavior of the function returning a plaintext
`string`, you can enable the configuration option `tenzir.legacy-secret-model`.
In this mode, the `secret` function can only resolve secrets from the Tenzir Node's
configuration file and not access any external secret store.

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
