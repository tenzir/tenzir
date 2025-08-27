

# General

## `series`, `table_slice` and expression evaluation

`series` and `table_slice` are columnar structures. The evaluator (`eval`) works
on entire columns or slices.

When working with these types, evaluate expressions once per series or slice.

If row-wise access is necessary, perform the access in a single iteration loop
after evaluation.

Any `ast::*` entity should be evaluated using an evaluator and not manually accessed.

## Secrets

The type `tenzir::secret` can only be used by resolving the secret value in an
operator.

This requires to be in an operator's generator loop, with access to the `operator_control_plane`, usually called `ctrl`.

In this example, the values `args_.url` and `args_.token` are `secret`s and are
resolved into the local variables.

```cpp
auto url = std::string{};
auto token = std::string{};
auto x = ctrl.resolve_secrets_must_yield({
  make_secret_request("url", args_.url, url,
                      dh),
  make_secret_request("token", args_.token, token,
                      dh),
});
co_yield std::move(x);
```

## Working with Generators

When iterating a generator in a loop, keep the iterator around. Do not regenerate it from `.begin()`.

## Use `view3.hpp`

When iterating or accessing series or arrow table slices, use the contents of `view3.hpp`.

Do not use the `value_at` facilities from arrow_table_slice.hpp or `.values()` methods.

## Do not materialize

Do not materialize elements of a series or array into a `record` or `list`. Perform
operations inside of the iteration loop on `series`

## Variant access

- Use `is` instead of `std::holds_alternative`
- Use `as` instead of `std::get`
- Use `try_as` instead of `std::get_if`

## Using match statements

A `match` function call can be used on a lot of types within Tenzir

```cpp
auto x = series{};
auto r = match( x,
  []( basic_series<double_type> ) {},
  []( basic_series<int_type> ) {},
  []( basic_series<uint_type> ) {},
  []( const auto& ) {
    /* Generic fallback */
  },
);
```
Prefer using `match` to perform operations based on the concrete content of
the types `data`, `series`, `tenzir::variant` and `std::variant`.
