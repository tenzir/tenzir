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

The Tenzir Platform stores a separate set of secrets for every workspace.
These secrets are accessible to all Tenzir Nodes connected to that workspace.

Secrets can be added, updated or deleted from a workspace using either the
`tenzir-platform` CLI or the web interface. Read more details in the [CLI
reference](../platform-cli.md#Manage Secrets).

For example, to add a new secret `geheim`, use the following command:

```bash
tenzir-platform secret add geheim --value=1528F9F3-FAFA-45B4-BC3C-B755D0E0D9C2
```

To manage secrets from the web interface, go to the `Workspace Settings` screen
by clicking on the gear icon in the workspace selector.

![Secrets UI](./screenshot.png)

#### External Secret Stores

:::note Sovereign Edition Recommended
The ability to use an external secret store is most useful for Sovereign Edition
customers running a self-hosted instance of the platform.
:::

Instead of using its internal secret store, the Tenzir Platform can be
configured to provide access to the secrets stored in an external secrets store
instead. This access is read-only, writing to or deleting from an external
secrets store is currently not supported.

External secret stores can only be configured using the `tenzir-platform` CLI.
To configure an external secret store, use the `secret store add` subcommand.

```bash
tenzir-platform secret store add aws --region='eu-west-1' --assumed-role-arn='arn:aws:iam::1234567890:role/tenzir-platform-secrets-access' --prefix=tenzir/
```

At the moment, only AWS Secrets Manager is supported as external secrets store.

In order to provide access the external secrets to the nodes, the Tenzir Platform
must be given the necessary permissions to read secret values from the external store.
In the example above, this means that the Tenzir Platform must be able to assume
the specified role, and the role must have permissions to read secrets under the
prefix `tenzir/` from the Secrets Manager instance in the account of the
assumed role.

See the [CLI reference](../platform-cli.md#Manage Secret Stores) for more details.
