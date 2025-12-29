# Secrets

Resolving secret values in Tenzir operators.

## Overview

The `tenzir::secret` type requires resolution within an operator's generator
loop with access to `operator_control_plane`.

## Resolution Pattern

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
    // Use url and token
    co_yield process(slice, url, token);
  }
}
```

## Key Points

- Resolution must happen inside the generator (use `co_yield`)
- `make_secret_request` takes: name, secret, output variable, diagnostic handler
- Multiple secrets can be resolved in a single call
- Resolved values are written to the provided string references
