# assert_secret

Checks a secret against an expected value.

:::danger
This operator should not be used outside of testing and is not available by default.
:::

## Description

The `assert_secret` operator checks a secret against an expected value.
It is only meant for testing. Since it can technically be used to brute force
a secret, it disabled by default.

You can enable this operator using the `tenzir.enable-assert-secret-operator`
configuration option.

### `secret=secret`

The secret to test.

### `expected=blob|string`

The expected value.

## Examples

The following will pass without any output or error:

```yaml title="Used configuration"
tenzir:
  enable-assert-secret-operator: true
  secrets:
    key: "value"
```
```tql title="Example usage"
from {}
assert_secret secret=secret("key"), expected="value"
```

## See Also

[`secret`](../functions/secret.md)
