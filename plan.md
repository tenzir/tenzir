# Plan: Port `package::list` to the New Executor

## Background

The `package::list` operator (formerly `packages`, renamed in v4.24.0 via PR #4741/#4746) does not currently have an implementation in this codebase. The old implementation lived in the platform-specific code and relied on the node's package manager actor via the REST API pattern.

The goal is to create a new-executor implementation that loads packages from the configured package directories (the same directories used by `load_packages_for_exec` in `libtenzir/src/tql2/exec.cpp`) and emits them as table slices.

## Architecture

**Pattern**: One-shot source (`Operator<void, table_slice>`), same as `sockets.cpp` and `plugins.cpp`.

**Key reference files**:
- `libtenzir/builtins/operators/plugins.cpp` — closest structural match (one-shot source, ported to new executor with `OperatorPlugin`)
- `libtenzir/builtins/operators/sockets.cpp` — another one-shot source reference
- `libtenzir/include/tenzir/package.hpp` — package data structures, `package::to_record()`
- `libtenzir/src/package.cpp` — `package::load()`, `package::to_record()`
- `libtenzir/src/tql2/exec.cpp:72-226` — `load_packages_for_exec()` shows how to discover and load packages from disk

## Steps

### Step 1: Create `libtenzir/builtins/operators/package_list.cpp`

Create a new file rather than modifying an existing one, since there is no existing implementation to extend.

### Step 2: Implement package loading helper

Extract or replicate the package directory discovery logic from `load_packages_for_exec()` (`exec.cpp:72-177`). This function:
1. Builds `package_dirs` from `config_dirs(sys.config())` + `"packages"` subdirectory
2. Reads `tenzir.package-dirs` from the config for additional directories
3. Iterates directories looking for `package.yaml` files
4. Calls `package::load(dir, dh, false)` for each (with `only_entities=false` to get full package info including pipelines/contexts)
5. Deduplicates by package ID

Write a local helper function (e.g., `load_all_packages`) that performs the same directory enumeration and returns `std::vector<package>`. Use `OpCtx& ctx` to access `ctx.actor_system().config()` for config directories and `ctx.dh()` for diagnostics.

**Important difference from `load_packages_for_exec`**: Pass `only_entities=false` to `package::load()` so the full package data (pipelines, contexts, etc.) is included in the output — unlike the exec path which strips them.

### Step 3: Implement `PackageListArgs` struct

```cpp
struct PackageListArgs {
  // No arguments — package::list takes no parameters.
};
```

### Step 4: Implement the `PackageList` operator

Follow the one-shot source pattern from `plugins.cpp`:

```cpp
class PackageList final : public Operator<void, table_slice> {
public:
  explicit PackageList(PackageListArgs /*args*/) {}

  auto start(OpCtx&) -> Task<void> override { co_return; }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    if (done_) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    co_return {};
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(result);
    auto packages = load_all_packages(ctx);
    auto builder = series_builder{};
    for (const auto& pkg : packages) {
      builder.data(pkg.to_record());
    }
    for (auto&& slice : builder.finish_as_table_slice("tenzir.package")) {
      co_await push(std::move(slice));
    }
    done_ = true;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::unspecified;
  }

  auto snapshot(Serde& serde) -> void override {
    serde("done", done_);
  }

private:
  bool done_ = false;
};
```

### Step 5: Implement the old `crtp_operator` (for backward compatibility)

Add a `package_list_operator` using `crtp_operator` alongside the new executor class. This follows the convention from the porting guide: "Keep the old `crtp_operator` in place for backward compatibility (TQL1 / serialized pipelines)."

```cpp
class package_list_operator final : public crtp_operator<package_list_operator> {
  // Similar logic but using generator<table_slice> and operator_control_plane
};
```

### Step 6: Register the plugin

```cpp
class plugin final
  : public virtual operator_plugin<package_list_operator>,  // TQL1
    public virtual operator_factory_plugin,                  // TQL2 old path
    public virtual OperatorPlugin {                          // TQL2 new path (new executor)
public:
  auto name() const -> std::string override {
    return "package_list";
  }

  auto signature() const -> operator_signature override {
    return {.source = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override { ... }
  auto make(operator_factory_invocation inv, session ctx) const -> failure_or<operator_ptr> override { ... }

  auto describe() const -> Description override {
    auto d = Describer<PackageListArgs, PackageList>{};
    return d.without_optimize();
  }
};
```

**Naming note**: The plugin `name()` should return `"package_list"` (or `"tql2.package_list"` if needed to avoid clashing with any existing registration). In TQL2, the operator is invoked as `package::list` via module-qualified naming — the `package` module with the `list` operator. Check how the module registration system maps this name. If `package::list` is resolved via module lookup rather than a flat plugin name, the plugin name just needs to not clash.

### Step 7: Add to CMakeLists.txt

Add `builtins/operators/package_list.cpp` to the sources list in `libtenzir/CMakeLists.txt`.

### Step 8: Includes

```cpp
#include <tenzir/operator_plugin.hpp>   // OperatorPlugin, Describer, etc. (also includes async.hpp)
#include <tenzir/package.hpp>           // package, package::load(), package::to_record()
#include <tenzir/series_builder.hpp>    // series_builder
#include <tenzir/pipeline.hpp>          // pipeline, crtp_operator
#include <tenzir/plugin.hpp>            // plugin registration
#include <tenzir/configuration.hpp>     // config_dirs()
#include <tenzir/tql2/plugin.hpp>       // operator_factory_plugin, argument_parser2
```

### Step 9: Testing

- Add integration tests under `test/` for `package::list`.
- Test with no packages installed (empty output).
- Test with a sample package directory containing a `package.yaml`.
- Test that the output schema matches `tenzir.package` with expected fields: `id`, `name`, `author`, `description`, `package_icon`, `author_icon`, `categories`, `config`, `operators`, `pipelines`, `contexts`, `examples`, `inputs`.

## Key Decisions & Considerations

1. **No args**: `package::list` takes no arguments, making this a straightforward one-shot source.

2. **Package discovery reuse**: The directory enumeration logic in `load_packages_for_exec()` should ideally be shared, but per the porting guide ("don't extract shared logic from the old implementation"), it's OK to have some duplication. Consider extracting the directory enumeration into a shared helper in `package.hpp`/`package.cpp` if it doesn't already exist, since both paths need it.

3. **`only_entities=false`**: Unlike the exec startup path which passes `only_entities=true` (to only load operators for the registry), `package::list` should pass `false` to get the full package information including pipelines and contexts.

4. **Schema name**: Use `"tenzir.package"` as the table slice schema name, consistent with other operators (e.g., `"tenzir.plugin"` for `plugins`, `"tenzir.version"` for `version`).

5. **Error handling**: If a package fails to load, emit a diagnostic warning and continue with the remaining packages rather than failing entirely.

6. **Snapshot**: Serialize the `done_` flag so the operator doesn't re-emit packages after a checkpoint restore.
