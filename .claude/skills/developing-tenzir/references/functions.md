# TQL Functions

Implementing TQL functions in Tenzir.

## Function Plugin Structure

```cpp
class my_function : public virtual function_plugin {
  auto name() const -> std::string override {
    return "my_function";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    // Parse arguments and return function
  }
};

TENZIR_REGISTER_PLUGIN(my_function)
```

## Argument Parsing

Use `argument_parser2::function()`:

```cpp
auto make_function(invocation inv, session ctx) const
  -> failure_or<function_ptr> override {
  auto expr = ast::expression{};
  auto opt = std::optional<located<int64_t>>{};
  TRY(argument_parser2::function(name())
        .positional("x", expr, "any")
        .named("limit", opt)
        .parse(inv, ctx));
  // Return function_use
}
```

## Function Implementation

Return `function_use::make()` with a lambda:

```cpp
return function_use::make(
  [expr = std::move(expr)](evaluator eval, session) -> series {
    const auto& input = eval(expr);
    auto b = string_type::make_arrow_builder(arrow_memory_pool());
    for (auto value : input.values()) {
      // Process each value
      check(b->Append(result));
    }
    return series{string_type{}, finish(*b)};
  });
```

## Key Points

- Evaluate expressions with `eval(expr)` inside the lambda
- Build output using Arrow builders with `check()`
- Return `series{type, finish(builder)}`
