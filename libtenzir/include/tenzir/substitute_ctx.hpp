//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/base_ctx.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/tql2/ast.hpp"

#include <unordered_map>

namespace tenzir {

/// Context when substituting let bindings with a constant.
class substitute_ctx {
public:
  using env_t = std::unordered_map<let_id, ast::constant::kind>;

  /// Construct a new context with the given environment.
  ///
  /// If `env == nullptr`, then an empty environment is assumed.
  substitute_ctx(base_ctx ctx, const env_t* env);

  /// Return the constant stored for the given `let`, if already known.
  auto get(let_id id) const -> std::optional<ast::constant::kind>;

  /// Return all constants that can be substituted with this context.
  auto env() const -> env_t;

  /// Return a new context that uses the given environment.
  auto with_env(const env_t* env) const -> substitute_ctx;

  explicit(false) operator diagnostic_handler&() {
    return ctx_;
  }

  explicit(false) operator const registry&() {
    return ctx_;
  }

private:
  base_ctx ctx_;
  const env_t* env_;
};

} // namespace tenzir
