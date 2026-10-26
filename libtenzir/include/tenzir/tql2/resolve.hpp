//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/base_ctx.hpp"
#include "tenzir/detail/heterogeneous_string_hash.hpp"
#include "tenzir/option.hpp"
#include "tenzir/session.hpp"
#include "tenzir/tql2/ast.hpp"

#include <string>
#include <string_view>
#include <unordered_map>

namespace tenzir {

/// The packages some code may name: each name it may use, mapped to the module
/// that holds that package under the `packages` root. In Rust's terms, the
/// extern prelude of the crate the code belongs to.
///
/// It decides only which packages code may name. Builtins are always there,
/// like Rust's standard library prelude.
class ExternPrelude {
public:
  /// No packages, as for code that a builtin writes.
  static auto empty() -> ExternPrelude;

  /// Every loaded package, by its own name.
  ///
  /// Deprecated: for code that has no workspace to declare what it uses, such
  /// as the pipelines of a node, or of `tenzir` outside of a workspace.
  static auto all_packages_deprecated() -> ExternPrelude;

  /// Lets code name the package in `module` as `name`.
  void add(std::string name, std::string module);

  /// The module that holds the package `name` stands for, unless code may not
  /// name it.
  auto module(std::string_view name) const -> Option<std::string_view>;

  /// Whether code may name every package.
  auto all() const -> bool;

private:
  ExternPrelude() = default;

  /// The modules by the names code uses for them, or none for every package.
  Option<std::unordered_map<std::string, std::string,
                            detail::heterogeneous_string_hash, std::equal_to<>>>
    modules_;
};

/// Resolves the entities that `pipe` names, letting it name the packages that
/// `prelude` holds. Code has to be resolved before it is compiled.
auto resolve_entities(ast::pipeline& pipe, ExternPrelude const& prelude,
                      base_ctx ctx) -> failure_or<void>;

auto resolve_entities(ast::pipeline& pipe, ExternPrelude const& prelude,
                      session ctx) -> failure_or<void>;

auto resolve_entities(ast::expression& expr, ExternPrelude const& prelude,
                      base_ctx ctx) -> failure_or<void>;

auto resolve_entities(ast::expression& expr, ExternPrelude const& prelude,
                      session ctx) -> failure_or<void>;

auto resolve_entities(ast::function_call& fc, ExternPrelude const& prelude,
                      session ctx) -> failure_or<void>;

/// Whether every entity that `pipe` names is resolved.
auto entities_resolved(ast::pipeline& pipe) -> bool;

} // namespace tenzir
