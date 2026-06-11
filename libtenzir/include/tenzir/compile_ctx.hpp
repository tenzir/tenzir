//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/base_ctx.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/let_id.hpp"
#include "tenzir/location.hpp"

namespace tenzir {

/// This context is used throughout the compilation process from AST to IR.
///
/// Its main responsibility is the name resolution of `let` bindings. The
/// context itself provides read-only access to the environment. New scopes can
/// be opened, which can then be used to modify the environment.
class compile_ctx {
public:
  class root;
  class scope;
  using env_t = std::unordered_map<std::string, let_id>;

  /// Create a new context, which is owned by the returned `root` object.
  ///
  /// The root object must be kept alive while the context is being used.
  static auto make_root(base_ctx ctx) -> root;

  /// Like `make_root`, but populates the externally owned `source_map` during
  /// compilation instead of an internally owned one. The referenced map must
  /// outlive the returned `root`. This lets a caller (e.g. the diagnostic
  /// printer) observe sources and call sites as they are registered.
  static auto make_root(base_ctx ctx, SourceMap& source_map) -> root;

  /// Open a new variable scope within this context.
  ///
  /// This operation modifies `this`, but not affect any previous copies. The
  /// returned object must be kept alive while `this` is then used.
  [[nodiscard]] auto open_scope() -> scope;

  /// Return the `let_id` for the given name, if it exists.
  auto get(std::string_view name) const -> std::optional<let_id>;

  /// Return the full environment containing all bindings.
  auto env() const -> env_t;

  /// Create a copy of this context, but without the environment.
  [[nodiscard]] auto without_env() const -> compile_ctx;

  /// Return the source map that is populated during compilation.
  auto source_map() const -> SourceMap& {
    return root_.source_map_ref();
  }

  /// A scope object owns the environment from which the context reads.
  ///
  /// When the scope is destroyed, the context's environment pointer is restored
  /// to its original value.
  class scope {
  public:
    ~scope();

    scope(scope&& other) noexcept;
    auto operator=(scope&& other) noexcept -> scope&;

    scope(const scope&) = delete;
    auto operator=(const scope&) -> scope& = delete;

    /// Provide a new binding with the given name, returning its `let_id`.
    auto let(std::string name) & -> let_id;

  private:
    friend class compile_ctx;

    scope(std::unique_ptr<env_t> env, compile_ctx* ctx,
          const env_t* original_env, root& root);

    // The environment stored in a `unique_ptr` because we remember its address
    // in the context when opening a new scope.
    std::unique_ptr<env_t> env_;
    compile_ctx* ctx_; // The context we modified (nullptr if moved-from)
    const env_t* original_env_; // The env_ pointer to restore on destruction
    root& root_;
  };

  /// There is a single root object that copies of the context use.
  class root {
  public:
    ~root() = default;
    root(const root&) = delete;
    auto operator=(const root&) -> root& = delete;
    root(root&&) = default;
    auto operator=(root&&) -> root& = default;

    operator base_ctx() const;
    operator compile_ctx();

    /// Extract the source map after compilation has finished. Only meaningful
    /// when the root owns its source map; if an external map was provided to
    /// `make_root`, that map is populated directly instead.
    auto source_map() && -> SourceMap {
      return std::move(source_map_);
    }

  private:
    friend class compile_ctx;

    explicit root(base_ctx ctx) : ctx_{ctx} {
    }

    root(base_ctx ctx, SourceMap& source_map)
      : ctx_{ctx}, external_source_map_{&source_map} {
    }

    /// Return the source map to populate: the external one if provided,
    /// otherwise the internally owned one.
    auto source_map_ref() -> SourceMap& {
      return external_source_map_ ? *external_source_map_ : source_map_;
    }

    base_ctx ctx_;
    uint64_t last_let_id_ = 0;
    SourceMap source_map_;
    SourceMap* external_source_map_ = nullptr;
  };

  auto reg() const -> const registry& {
    return *this;
  }

  explicit(false) operator const registry&() const {
    return root_.ctx_;
  }

  explicit(false) operator diagnostic_handler&() const {
    return root_.ctx_;
  }

private:
  compile_ctx(root& root, const env_t* env);

  root& root_;
  const env_t* env_;
};

/// The result of compiling an AST pipeline into IR.
struct CompiledPipeline {
  /// The compiled IR pipeline.
  ir::pipeline ir;

  /// The source map populated during compilation.
  SourceMap source_map;
};

/// Compile an AST pipeline into IR.
///
/// This is the entry point into compilation. It creates the compilation
/// context internally and returns the resulting IR together with the source
/// map that was populated during compilation.
auto compile(ast::pipeline ast, base_ctx ctx) -> failure_or<CompiledPipeline>;

/// Compile an AST pipeline into IR, populating the externally owned
/// `source_map` during compilation. The referenced map must outlive the call.
/// This allows a diagnostic printer that already references `source_map` to
/// resolve locations into sources registered during compilation (e.g. the
/// bodies of user-defined operators).
auto compile(ast::pipeline ast, base_ctx ctx, SourceMap& source_map)
  -> failure_or<ir::pipeline>;

} // namespace tenzir
