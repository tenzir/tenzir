# Secrets

Secrets can be used in two ways:

* By providing a plain `string` to an operator that expects a secret.
* By obtaining a secret from a secret store, using the [`secret`](../tql2/functions/secret.md) function

 <!-- TODO: make decision about how to document arguments. If we dont document which can be secrets, we may want to express this here. -->

```tql

```


## Ad-hoc Secrets

Ad-hoc secrets are secrets that are created from a `string` directly within TQL.
This happens when you provide a `string` to an operator that expects a `secret`.

Providing plain string values can be useful when developing pipelines.
<!-- TODO: Do we want to make some arguments, such as `url`s secrets? If so, we may want to mention this here. -->

It is important to understand that secrets created from plain `string`s do not
enjoy the same security as secrets obtained from secret store. Their value is
directly available in the TQL pipeline definition and the compiled and executed
representation. As such, it may be persisted on the node.

## Secret Lookup

The [`secret`](../tql2/functions/secret.md) function retrieves a secret.

Secrets are looked up in the following order:

1. The environment of the currently running process ¹
2. The configuration of the currently running process ¹
3. The environment of the Tenzir Node process ²
4. The configuration of the Tenzir Node process ²
5. The Tenzir Platform secret store for the Workspace the Tenzir Node is running in ² ³

¹⁾ Steps 1 & 2 apply to the `tenzir` client process only. If a pipeline is executed
directly in a Tenzir Node, they are equivalent to steps 3 & 4.

²⁾ Steps 3-5 require a Tenzir Node to connect to.

³⁾ Step 5 requires a secret store to be configured in the Tenzir Platform.

A secrets actual value is only looked up when it is required by the operator. If
the value is looked up over any network connection, it is additionally encrypted
using ECIES with a one-time, per secret key. The value stays encrypted through
the entire transfer until the final usage site.

## Tenzir configuration secrets

Secrets can be specified in the `tenzir.yaml` config file, under the path
`tenzir.secrets`.

```yaml
tenzir:
  secrets:
    # Add your secrets there.
    geheim: 1528F9F3-FAFA-45B4-BC3C-B755D0E0D9C2
```

Because Tenzir's configuration options can also be set as environment variables,
this means that secret can also be defined in the environment. The above secret
could also be defined as `TENZIR_SECRETS__GEHEIM`
