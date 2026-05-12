# TQL operators

Implementing TQL operator plugins in Tenzir.

Use the executor API in [executor.md](./executor.md) for operator
implementations.

## Plugin declaration

Use `OperatorPlugin` and `Describer` to register operators:

```cpp
struct Args {
  located<uint64_t> capacity = {};
  Option<located<std::string>> policy;
};

class plugin final : public virtual OperatorPlugin {
public:
  auto describe() const -> Description override {
    auto d = Describer<Args, Impl>{};
    auto cap = d.positional("capacity", &Args::capacity);
    d.validate([cap](DescribeCtx& ctx) -> Empty {
      TRY(auto c, ctx.get(cap));
      if (c.inner == 0) {
        diagnostic::error("capacity must be greater than zero")
          .primary(c.source).emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }
};
```

Use `located<T>` when diagnostics need the source location. Use
`Option<located<T>>` for optional named arguments.

`DescribeCtx::get()` returns an empty option when the caller omitted an
argument, even if the target `Args` member has a default initializer. Apply
defaults explicitly in validation callbacks.

Use `d.order_invariant()` only for pure row transforms that can be reordered.
Use `d.without_optimize()` otherwise.

Use `d.spawner(...)` only when validation or instantiation depends on the input
type.

## Diagnostics

If diagnostics need the operator location, store it in `Args` and register it:

```cpp
d.operator_location(&Args::operator_location);
```

Prefer adding a primary location over rewriting diagnostic messages to mention
the operator.

## Secret resolution

Resolve secrets through `OpCtx`:

```cpp
auto resolved = std::string{};
auto requests = std::vector<secret_request>{
  make_secret_request("url", args_.url, resolved, ctx.dh()),
};
CO_TRY(co_await ctx.resolve_secrets(std::move(requests)));
```
