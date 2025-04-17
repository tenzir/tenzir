# secret

Use the value of a secret.

```tql
secret(name:string) -> secret
```

## Description

The `secret` function retrieves the value associated with the key `name`.

The secret is first looked up in the node's config file, If it is not found there,
a request is made to the Tenzir Platform.
Should the platform also not be able to find the secret, an error is raised.

### `name: string`

The name of the secret to use.

## Lookup Order

Secrets are looked up in the following order:

1. The environment of the currently running process
2. The configuration of the currently running process
3. The environment of the Tenzir Node process
4. The configuration of the Tenzir Node process
5. The Tenzir Platform secret store for the Workspace the Tenzir Node is running in

### Tenzir configuration secrets

Secrets can be specified in the `tenzir.yaml` config file, under the path
`tenzir.secrets`.

```yaml
tenzir:
  secrets:
    # Add your secrets there.
    geheim: 1528F9F3-FAFA-45B4-BC3C-B755D0E0D9C2
```

## Examples

### Using secrets in an operator

```tql
load_tcp "127.0.0.1:4000",{
  read_ndjson
}
to_splunk secret("splunk_hec_endpoint"), hec_token=secret("splunk_hec_token")
```

### Secrets are not rendered in output

```tql
from {x: secret("geheim")}
```
```tql
{x: secret("geheim")}
```

## See also

[`config`](config.md),
[`env`](env.md)
