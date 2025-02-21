//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/base_ctx.hpp"
#include "tenzir/let_id.hpp"

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

  /// A scope object owns the environment from which the context reads.
  class scope {
  public:
    /// Provide a new binding with the given name, returning its `let_id`.
    auto let(std::string name) & -> let_id;

  private:
    friend class compile_ctx;

    scope(std::unique_ptr<env_t> env, root& root);

    // The environment stored in a `unique_ptr` because we remember its address
    // in the context when opening a new scope.
    std::unique_ptr<env_t> env_;
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

  private:
    friend class compile_ctx;

    explicit root(base_ctx ctx) : ctx_{ctx} {
    }

    base_ctx ctx_;
    uint64_t last_let_id_ = 0;
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

  explicit(false) operator caf::actor_system&() const {
    return root_.ctx_;
  }

private:
  compile_ctx(root& root, const env_t* env);

  root& root_;
  const env_t* env_;
};

} // namespace tenzir
