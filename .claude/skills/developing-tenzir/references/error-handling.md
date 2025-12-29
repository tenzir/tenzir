# Error Handling

Error handling patterns in Tenzir.

## TRY Macro

Use `TRY()` for early return on error with `failure_or<T>` (or legacy `caf::expected<T>`):

```cpp
auto make_thing(args) -> failure_or<thing> {
  TRY(auto validated, validate(args));
  TRY(auto parsed, parse(validated));
  return thing{parsed};
}
```

With variable binding:

```cpp
TRY(auto result, some_operation());
// result is unwrapped value
```

Without binding (for void operations):

```cpp
TRY(validate_something());
```

## check() for Assertions

Use `check()` if you can and `TENZIR_ASSERT` if you check boolean-like values:

```cpp
// For Arrow operations
check(builder.Append(value));

// For expected values
auto array = check(builder.Finish());
```

`check()` captures source location for better error messages.

## Return Types

- `failure_or<T>` — For functions that emit diagnostics
- `caf::expected<T>` — Legacy type for functions returning errors
- Use `[[nodiscard]]` on functions where ignoring errors is a bug

## Pattern

```cpp
auto process(input) -> failure_or<output> {
  TRY(auto step1, validate(input));
  TRY(auto step2, transform(step1));
  return step2;
}
```
