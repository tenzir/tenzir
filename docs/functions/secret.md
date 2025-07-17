---
title: secret
category: Runtime
example: 'secret("KEY")'
---

Use the value of a secret.

```tql
secret(name:string) -> secret
```

## Description

An operator accepting a secret will first try and lookup the value in the
environment or configuration of the Tenzir Node.
A `tenzir` client process can use secrets only if it has a Tenzir Node to connect
to.

If the secret is not found in the node, a request is made to the Tenzir Platform.
Should the platform also not be able to find the secret, an error is raised.

See the [explanation page for secrets](/explanations/secrets) for more
details.

### `name: string`

The name of the secret to use. This must be a constant.

## Legacy Model

The configuration option `tenzir.legacy-secret-model` changes the behavior of
the `secret` function to return a `string` instead of a `secret`.

The legacy model only allows using secrets from the Tenzir Node's configuration.
No secrets from the Tenzir Platform's secret store will be available.

We do not recommend enabling this option.

## Examples

### Using secrets in an operator

```tql
load_tcp "127.0.0.1:4000" {
  read_ndjson
}
to_splunk "https://localhost:8088", hec_token=secret("splunk-hec-token")
```

### Secrets are not rendered in output

```tql
from {x: secret("geheim")}
```

```tql
{x: "***" }
```

## See also

[`config`](/reference/functions/config),
[`env`](/reference/functions/env)
