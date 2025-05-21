The return value of the `secret` is no longer a plain `string`, but a special
type that allows us to ensure no conversion to `string` is possible. Operators
accept this special type and obtain the value on when necessary. The previous
behavior can be be enabled using the configuration option
`tenzir.legacy-secret-model`.

The Tenzir Node can now use the Tenzir Platforms secret store through the
`secret` function. This is not possible using the the legacy secret model.
