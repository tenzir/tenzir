# secret

Use the value of a secret.

```tql
secret(name:string) -> secret
```

## Description

The `secret` function retrieves the value associated with the key `name`.

The secret is first looked up locally in the environment or configuration of the
Tenzir Node. A `tenzir` client process can use secrets only if it has a Tenzir
Node to connect to.
If the secret is not found in the Node, a request is made to the Tenzir Platform.
Should the platform also not be able to find the secret, an error is raised.

See the [Explanation page for secrets](../../../docs/secrets/README.md) for more
details.

### `name: string`

The name of the secret to use. This must be a constant.

## Examples

### Using secrets in an operator

```tql
load_tcp "127.0.0.1:4000",{
  read_ndjson
}
to_splunk "localhost", hec_token=secret("splunk_hec_token")
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
