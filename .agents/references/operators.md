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

Declare only consumed runtime inputs; bind separately from rewrite policy:

```cpp
// <tenzir/operator/optimization.hpp>
struct Args {
  OptimizationArgs<opt::Filter, opt::Limit, opt::Projection> optimization;
};
d.optimization(&Args::optimization);
return d.without_optimize();
```

- Access `args.optimization.filter`, `.limit`, `.projection`, or `.order`.
  Order-only: `OptimizationArgs<opt::Order>`. Rewrite-only: no bundle.
- Binding enables no propagation. Keep user arguments outside the bundle.
- Filters: enforce every accepted predicate in order; push exact translations,
  evaluate the rest with `filter2`.
- Limits: require filter binding; count after filtering. Push into SQL only
  with the entire chain. Track progress separately from immutable arguments.
- Projections: `None` means all fields; empty means none. Retain filter and
  computation dependencies.

Examples: `from_clickhouse`, `export`. Document supported modes, pushed/local
predicates, and `--dump-opt-ir` verification in the source's `## Optimizations`
reference section, between arguments and examples.

Use `d.spawner(...)` only when validation or instantiation depends on the input
type.

## Bespoke IR operators

An operator that customizes planning or optimization beyond what a
`Description` expresses implements `ir::Operator` itself. Parse its arguments
with `OperatorArguments` instead of `argument_parser2`:

```cpp
auto describe_my_op() -> Description {
  auto d = Describer<Args>{};
  d.name("my_op");
  d.operator_location(&Args::keyword);
  d.positional("capacity", &Args::capacity);
  d.pipeline(SubOptimize::off);
  d.inline_pipeline();
  return d.only_arguments();
}

using MyArguments = OperatorArguments<Args, describe_my_op>;
```

Hold `MyArguments` as the operator's state, forward `substitute()` to it, and
call `get()` for the typed arguments. Arguments that are constants only resolve
during substitution, so `infer_type()` may only use the operator location, the
subpipeline via `pipe()`, and arguments declared as `ast::expression`.

Declare the subpipeline without a member pointer and read it through `pipe()`
or `take_pipe()`. Materializing it into `Args` deep-copies the subpipeline on
every `get()`, which is quadratic in the nesting depth. Call
`d.inline_pipeline()` when the subpipeline is part of the enclosing pipeline
instead of being bound at runtime.

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
