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

A source that can act on optimizer hints opts in with `d.optimize_filter(...)`,
`d.optimize_limit(...)`, and `d.optimize_projection(...)`. Opting into the
filter moves the entire downstream filter chain into the operator, so the
operator must apply every predicate: push what it can translate exactly and
evaluate the rest locally with `filter2`. The limit counts events after the
whole chain, so push it only when the chain went along. See `from_clickhouse`
for the pattern and `export` for the storage-engine variant.

Every source that opts in documents an `## Optimizations` section on its
reference page in `tenzir/content`, placed after the argument descriptions and
before the examples. State which hints the operator acts on and in which modes,
which predicates it pushes and which stay local, and how to verify with
`tenzir --dump-opt-ir`. Describe the contract, not the mechanism: what a user
can rely on, never how the operator achieves it.

Use `d.spawner(...)` only when validation or instantiation depends on the input
type.

## Reader auto-detection

When you add a parser, account for `read_auto` in the same change. If the format
can be identified reliably, add automatic detection and integration tests under
`test/tests/operators/read_auto`. Include incremental-input coverage when
applicable. Otherwise, explain in the change description why automatic
detection is unsafe.

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
