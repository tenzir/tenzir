# TQL Operators

Implementing TQL operators in Tenzir.

## Operator Plugin Structure

```cpp
class my_operator final : public crtp_operator<my_operator> {
public:
  my_operator() = default;

  explicit my_operator(config cfg) : cfg_{std::move(cfg)} {}

  auto operator()(generator<table_slice> input,
                  operator_control_plane& ctrl) const
    -> generator<table_slice> {
    for (const auto& slice : input) {
      // Process slice
      co_yield result;
    }
  }

  auto name() const noexcept -> std::string override {
    return "my_operator";
  }

  friend auto inspect(auto& f, my_operator& x) -> bool {
    return f.apply(x.cfg_);
  }

private:
  config cfg_;
};
```

## Generator Best Practices

Keep iterators alive across loop iterations:

```cpp
auto operator()(generator<table_slice> input,
                operator_control_plane& ctrl) const
  -> generator<table_slice> {
  // Do NOT call input.begin() multiple times
  for (const auto& slice : input) {
    co_yield process(slice);
  }
}
```

## Operator Control Plane

Use `ctrl` for diagnostics and control:

```cpp
auto operator()(..., operator_control_plane& ctrl) const
  -> generator<table_slice> {
  auto dh = ctrl.diagnostics();
  dh.emit(diagnostic::warning("something happened")
            .primary(location)
            .done());
  // ...
}
```

## Plugin Registration

```cpp
class plugin final : public virtual operator_plugin<my_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }
  // ...
};

TENZIR_REGISTER_PLUGIN(plugin)
```

## Secret Resolution

When your operator needs secrets (API keys, credentials), resolve them inside
the generator loop using `operator_control_plane`:

```cpp
auto operator()(generator<table_slice> input,
                operator_control_plane& ctrl) const
  -> generator<table_slice> {
  auto dh = ctrl.diagnostics();

  // Declare variables to receive resolved values
  auto url = std::string{};
  auto token = std::string{};

  // Resolve secrets (yields until complete)
  auto x = ctrl.resolve_secrets_must_yield({
    make_secret_request("url", args_.url, url, dh),
    make_secret_request("token", args_.token, token, dh),
  });
  co_yield std::move(x);

  // Now url and token contain resolved values
  for (const auto& slice : input) {
    co_yield process(slice, url, token);
  }
}
```

Key points:

- Resolution must happen inside the generator (use `co_yield`)
- `make_secret_request` takes: name, secret, output variable, diagnostic handler
- Multiple secrets can be resolved in a single call
