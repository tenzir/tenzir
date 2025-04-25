# Secrets

Secrets are accepted by operators for parameters that may be sensitive,
such as authentication tokens, passwords or even URLs.

There is two ways to pass an argument to a parameter that expects a secret:

- By providing a plain `string` (Ad-hod secret):

  ```tql
  to_splunk "localhost", hec_token="my-plaintext-token"
  ```

- By using the [`secret`](../tql2/functions/secret.md) function (Manged secret):

  ```tql
  to_splunk "localhost", hec_token=secret("splunk-hec-token")
  ```

  This will fetch the value of the secret named `splunk-hec-token`.

<!-- TODO: Do we want this? -->

Operators generally do not document that they accept a secret, but will accept
secrets where appropriate.

## The `secret` type

Secrets are a special type in Tenzir's type system. They practically none of
operations you can perform on other values in Tenzir and can effectively only be
passed to operators.

This means that you _cannot_ do operations like `secret("my-secret") + "added text"`.

:::info Secrets are UTF-8
All secrets are UTF-8 unless you use the `decode_base64` function.
:::

The only operation you can perform on a secret is to `decode_base64` it, which
can be done at most once on a secret:

```tql
secret("my-secret").decode_base64()
```

## Ad-hoc Secrets

Ad-hoc secrets are secrets that are created from a `string` directly within TQL.
This happens when you provide a `string` to an operator that expects a `secret`.

Providing plain string values can be useful when developing pipelines, if you do
not want to add the secret to the configuration or a secret store.

<!-- TODO: Do we want this? -->

It is also useful for arguments that are not a secret for all users, such as URLs.

It is important to understand that secrets created from plain `string`s do not
enjoy the same security as managed secrets. Their value is directly available in
the TQL pipeline definition, as well as the compiled and executed representation.
As such, it may be persisted on the node.

## Managed Secrets

The [`secret`](../tql2/functions/secret.md) function retrieves a managed secret.

Secrets are looked up in the following order:

1. The environment of the Tenzir Node
2. The configuration of the Tenzir Node
3. The Tenzir Platform secret store for the Workspace the Tenzir Node is running in

A secrets actual value is only looked up when it is required by the operator
accepting a secret. If the value is looked up over any network connection, it is
additionally encrypted using ECIES with a one-time, per secret key.
The value stays encrypted through the entire transfer until the final usage site.

A `tenzir` client process can use the `secret` function only if it has a Tenzir
Node to connect to.

### Tenzir Configuration Secrets

Secrets can be specified in the `tenzir.yaml` config file, under the path
`tenzir.secrets`.

```yaml
tenzir:
  secrets:
    # Add your secrets there.
    geheim: 1528F9F3-FAFA-45B4-BC3C-B755D0E0D9C2
```

Since Tenzir's configuration options can also be set as environment variables,
this means that secrets can also be defined in the environment. The above secret
could also be defined via the environment variable `TENZIR_SECRETS__GEHEIM`.
An environment variable takes precedence over an equivalent key in the
configuration file.

See the [configuration reference](../configuration.md) for more details.

Be aware that the `tenzir.secrets` section is hidden from the
[`config()`](../tql2/functions/config.md) function.

### Platform Secrets


:::danger UNFINISHED
<!-- TODO: @platform-squad -->
:::
