//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/resolve.hpp"

#include "tenzir/compile_ctx.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/similarity.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/registry.hpp"

#include <tsl/robin_map.h>

#include <algorithm>
#include <array>
#include <ranges>
#include <unordered_set>

namespace tenzir {

namespace {

/// Modules that used to hold builtin entities. Modules are now reserved for
/// packages, and the entities that lived in these modules were renamed from
/// `mod::name` to `mod_name`. We keep the old spellings working with a
/// deprecation warning that relays to the new name.
///
/// TODO: Remove this compatibility shim with the next major release.
constexpr auto deprecated_builtin_modules = std::array<std::string_view, 5>{
  "ai", "context", "ocsf", "package", "pipeline"};

/// The flat name that replaced a formerly module-qualified builtin entity.
auto flatten_deprecated_path(const std::vector<ast::identifier>& path)
  -> Option<std::string> {
  if (path.size() != 2) {
    return None{};
  }
  if (std::ranges::find(deprecated_builtin_modules, path[0].name)
      == deprecated_builtin_modules.end()) {
    return None{};
  }
  return fmt::format("{}_{}", path[0].name, path[1].name);
}

class entity_resolver : public ast::visitor<entity_resolver> {
public:
  entity_resolver(const registry& reg, diagnostic_handler& diag,
                  ExternPrelude const& prelude)
    : reg_{reg}, diag_{diag}, prelude_{prelude} {
  }

  void visit(ast::entity& x) {
    if (x.ref.resolved()) {
      return;
    }
    TENZIR_ASSERT(not x.path.empty());
    TENZIR_ASSERT(context_ != context_t::none);
    // We use the following logic:
    // - Look at the first segment.
    // - Use its name + namespace (mod/fn/op) for lookup as user-defined.
    // - If something is found: Use rest of the segments. If not found: Error.
    // - If nothing is found: Repeat with lookup for built-in entities.
    const auto target_ns
      = context_ == context_t::op_name ? entity_ns::op : entity_ns::fn;
    const auto first_ns = x.path.size() == 1 ? target_ns : entity_ns::mod;
    const auto report_not_found = [&](const std::vector<ast::identifier>& path,
                                      size_t idx, entity_ns ns) {
      result_ = failure::promise();
      const auto available = std::invoke([&] {
        switch (ns) {
          case entity_ns::op:
            return reg_.operator_names();
          case entity_ns::fn:
            return reg_.function_names();
          case entity_ns::mod:
            return reg_.module_names();
          case entity_ns::let:
            return reg_.entity_names(entity_ns::let);
        }
        TENZIR_UNREACHABLE();
      });
      const auto type = std::invoke([&] {
        switch (ns) {
          case entity_ns::op:
            return "operator";
          case entity_ns::fn:
            return "function";
          case entity_ns::mod:
            return "module";
          case entity_ns::let:
            return "value";
        }
        TENZIR_UNREACHABLE();
      });
      const auto prefix = fmt::format(
        "{}", fmt::join(std::views::transform(path, &ast::identifier::name)
                          | std::views::take(idx),
                        "::"));
      const auto full = fmt::format(
        "{}", fmt::join(std::views::transform(path, &ast::identifier::name)
                          | std::views::take(idx + 1),
                        "::"));
      const auto& ident = path.at(idx);
      auto module_entities = std::vector<std::string>{};
      if (not prefix.empty()) {
        auto prefix_with_sep = fmt::format("{}::", prefix);
        for (const auto& candidate : available) {
          if (candidate.starts_with(prefix_with_sep)) {
            auto remainder = candidate.substr(prefix_with_sep.size());
            if (not remainder.empty()) {
              module_entities.push_back(std::string{remainder});
            }
          }
        }
        std::ranges::sort(module_entities);
      }
      auto type_plural = fmt::format("{}s", type);
      auto builder = diagnostic::error("{} `{}` not found", type, ident.name)
                       .primary(ident);
      if (available.empty()) {
        builder = std::move(builder).note("no {} found", type_plural);
      }
      auto suggestion = Option<std::string>{};
      if (not module_entities.empty()) {
        auto target = path.at(idx).name;
        auto best
          = std::ranges::max(module_entities, {}, [&](const auto& cand) {
              return detail::calculate_similarity(target, cand);
            });
        if (detail::calculate_similarity(target, best) > -5) {
          suggestion
            = prefix.empty() ? best : fmt::format("{}::{}", prefix, best);
        }
        builder
          = std::move(builder).note("available {} in `{}`: {}", type_plural,
                                    prefix, fmt::join(module_entities, ", "));
      } else if (not available.empty()) {
        auto best = std::ranges::max(available, {}, [&](const auto& cand) {
          return detail::calculate_similarity(full, cand);
        });
        if (detail::calculate_similarity(full, best) > -5) {
          suggestion = best;
        }
      }
      if (suggestion) {
        builder = std::move(builder).hint("did you mean `{}`?", *suggestion);
      }
      builder = std::move(builder).docs(
        "https://tenzir.com/docs/reference/{}",
        target_ns == entity_ns::op ? "operators" : "functions");
      std::move(builder).emit(diag_);
    };
    // Relay a formerly module-qualified builtin entity to its flat name, with a
    // deprecation warning. Package entities take precedence, so this runs only
    // after regular resolution failed.
    const auto relay_deprecated = [&]() -> bool {
      auto flat = flatten_deprecated_path(x.path);
      if (not flat) {
        return false;
      }
      for (auto pkg : {std::string{entity_pkg_cfg}, std::string{entity_pkg_std},
                       std::string{entity_pkg_deprecated}}) {
        auto path = entity_path{pkg, {*flat}, target_ns};
        if (not is<entity_ref>(reg_.try_get(path))) {
          continue;
        }
        auto old = fmt::format(
          "{}", fmt::join(std::views::transform(x.path, &ast::identifier::name),
                          "::"));
        auto loc = x.get_location();
        auto builder = diagnostic::warning("`{}` is deprecated", old)
                         .primary(loc)
                         .note("modules are reserved for packages");
        // Entities in the deprecated package exist only under their old
        // spelling, so there is no flat name to point at.
        if (pkg != entity_pkg_deprecated) {
          builder = std::move(builder).hint("use `{}` instead", *flat);
        }
        std::move(builder)
          .docs("https://tenzir.com/docs/reference/{}",
                target_ns == entity_ns::op ? "operators" : "functions")
          .emit(diag_);
        x.path = {ast::identifier{*flat, loc}};
        x.ref = std::move(path);
        return true;
      }
      return false;
    };
    // What the first segment stands for: itself, unless it names a package,
    // which the prelude maps to the module that holds it.
    auto first = x.path[0].name;
    auto pkg = std::invoke([&]() -> Option<entity_pkg> {
      // Resolution precedence: cfg (user overrides) -> std (builtins) ->
      // packages (installed)
      for (auto pkg :
           {std::string{entity_pkg_cfg}, std::string{entity_pkg_std}}) {
        auto path = entity_path{pkg, {first}, first_ns};
        if (is<entity_ref>(reg_.try_get(path))) {
          return pkg;
        }
      }
      if (auto module = prelude_.module(first)) {
        auto path = entity_path{
          std::string{entity_pkg_packages}, {std::string{*module}}, first_ns};
        if (is<entity_ref>(reg_.try_get(path))) {
          first = std::string{*module};
          return std::string{entity_pkg_packages};
        }
      }
      return None{};
    });
    if (not pkg) {
      // A package that code may not name is not there for it, so a
      // deprecated builtin spelling relays even if such a package exists.
      if (relay_deprecated() or report_undeclared(x.path[0], first_ns)) {
        return;
      }
      report_not_found(x.path, 0, first_ns);
      return;
    }
    // After figuring out which package it belongs to, we try the full path.
    auto segments = std::vector<std::string>{};
    segments.reserve(x.path.size());
    segments.push_back(std::move(first));
    for (auto& segment : x.path | std::views::drop(1)) {
      segments.push_back(segment.name);
    };
    auto path = entity_path{*pkg, std::move(segments), target_ns};
    auto result = reg_.try_get(path);
    auto err = try_as<registry::error>(result);
    if (err) {
      if (relay_deprecated()) {
        return;
      }
      TENZIR_ASSERT(err->segment < x.path.size());
      auto is_last = err->segment == path.segments().size() - 1;
      auto error_ns = is_last ? target_ns : entity_ns::mod;
      report_not_found(x.path, err->segment, error_ns);
      return;
    }
    x.ref = std::move(path);
  }

  void visit(ast::pkg_dollar_var& x) {
    if (x.value) {
      return;
    }
    // The full path is the module segments followed by the binding name, the
    // first segment being the package as the prelude maps it.
    auto module = prelude_.module(x.path[0].name);
    if (not module) {
      if (not report_undeclared(x.path[0], entity_ns::mod)) {
        diagnostic::error("package `{}` not found", x.path[0].name)
          .primary(x.path[0])
          .emit(diag_);
        result_ = failure::promise();
      }
      return;
    }
    auto segments = std::vector<std::string>{};
    segments.reserve(x.path.size() + 1);
    segments.emplace_back(*module);
    for (auto& segment : x.path | std::views::drop(1)) {
      segments.push_back(segment.name);
    }
    segments.push_back(std::string{x.name_without_dollar()});
    auto display = std::string{};
    for (const auto& segment : x.path) {
      display += segment.name;
      display += "::";
    }
    display += x.id.name;
    // Package `let`s live only in the `packages` domain.
    auto path
      = entity_path{std::string{entity_pkg_packages}, segments, entity_ns::let};
    auto result = reg_.try_get(path);
    auto* ref = try_as<entity_ref>(&result);
    if (not ref) {
      diagnostic::error("package binding `{}` not found", display)
        .primary(x.get_location())
        .emit(diag_);
      result_ = failure::promise();
      return;
    }
    auto* def = try_as<std::reference_wrapper<const ast::expression>>(ref);
    TENZIR_ASSERT(def);
    // Detect cycles across package `let` references (e.g. `a -> b -> a`).
    auto key = fmt::format("{}", fmt::join(segments, "::"));
    if (not resolving_.insert(key).second) {
      diagnostic::error("cyclic package binding `{}`", display)
        .primary(x.get_location())
        .emit(diag_);
      result_ = failure::promise();
      return;
    }
    // Resolve the binding's expression here, where the full registry is
    // available, then cache the constant result on the node. A package is
    // resolved before the packages that use it, so a binding of another
    // package is resolved already, against what that package may name.
    auto expr = def->get();
    visit(expr);
    resolving_.erase(key);
    if (result_.is_error()) {
      return;
    }
    // A package `let` is a compile-time constant. Reject bindings that would
    // evaluate differently each time (e.g. `random()`) so that every reference
    // yields the same value. Input references (e.g. `this`) are deterministic
    // and reach const-eval below, where they get a precise diagnostic.
    if (not expr.is_deterministic(reg_)) {
      diagnostic::error("package binding `{}` is not a constant", display)
        .primary(expr.get_location())
        .note("package `let` bindings must be deterministic")
        .emit(diag_);
      result_ = failure::promise();
      return;
    }
    // Non-value bindings (a lambda, `_`, a spread, an assignment) are not
    // rejected here: like pipeline `let` bindings, they reach const-eval, which
    // rejects them.
    auto value = const_eval(expr, base_ctx{diag_, reg_});
    if (not value) {
      result_ = failure::promise();
      return;
    }
    x.ref = std::move(path);
    x.value = std::move(value).unwrap().inner;
  }

  void visit(ast::invocation& x) {
    auto prev = std::exchange(context_, context_t::op_name);
    visit(x.op);
    context_ = prev;
    for (auto& y : x.args) {
      visit(y);
    }
  }

  void visit(ast::function_call& x) {
    auto prev = std::exchange(context_, context_t::fn_name);
    visit(x.fn);
    context_ = prev;
    for (auto& y : x.args) {
      visit(y);
    }
  }

  void visit(ast::type_stmt& x) {
    diagnostic::error(
      "type declarations are not yet supported within pipelines")
      .primary(x.type_location)
      .hint("put it into a `.tql` file in the schema directory")
      .emit(diag_);
    result_ = failure::promise();
  }

  void visit(ast::type_expr& x) {
    diagnostic::error("type expressions are not yet supported")
      .primary(x.keyword)
      .emit(diag_);
    result_ = failure::promise();
  }

  template <class T>
  void visit(T& x) {
    enter(x);
  }

  auto get_failure() -> failure_or<void> {
    return result_;
  }

private:
  /// Reports `name` as a package that exists but that the prelude does not
  /// let code name, if it is one.
  auto report_undeclared(ast::identifier const& name, entity_ns ns) -> bool {
    if (prelude_.all()) {
      return false;
    }
    auto path = entity_path{std::string{entity_pkg_packages}, {name.name}, ns};
    if (not is<entity_ref>(reg_.try_get(path))) {
      return false;
    }
    diagnostic::error("package `{}` is not among the dependencies", name.name)
      .primary(name)
      .hint("add it to `dependencies` in `package.yaml`")
      .emit(diag_);
    result_ = failure::promise();
    return true;
  }

  enum class context_t { none, op_name, fn_name };
  const registry& reg_;
  diagnostic_handler& diag_;
  ExternPrelude const& prelude_;
  context_t context_ = context_t::none;
  failure_or<void> result_;
  // Package `let` paths currently being resolved, to detect reference cycles.
  std::unordered_set<std::string> resolving_;
};

/// Finds whether some entity in what it visits is not resolved yet.
class ResolutionCheck : public ast::visitor<ResolutionCheck> {
public:
  void visit(ast::entity& x) {
    resolved_ = resolved_ and x.ref.resolved();
  }

  void visit(ast::pkg_dollar_var& x) {
    resolved_ = resolved_ and x.value.is_some();
  }

  template <class T>
  void visit(T& x) {
    enter(x);
  }

  auto resolved() const -> bool {
    return resolved_;
  }

private:
  bool resolved_ = true;
};

template <class T>
auto resolve(T& x, ExternPrelude const& prelude, registry const& reg,
             diagnostic_handler& dh) -> failure_or<void> {
  auto resolver = entity_resolver{reg, dh, prelude};
  resolver.visit(x);
  return resolver.get_failure();
}

} // namespace

auto ExternPrelude::empty() -> ExternPrelude {
  auto result = ExternPrelude{};
  result.modules_.emplace();
  return result;
}

auto ExternPrelude::all_packages_deprecated() -> ExternPrelude {
  return ExternPrelude{};
}

void ExternPrelude::add(std::string name, std::string module) {
  TENZIR_ASSERT(modules_, "every package is already in the prelude");
  modules_->insert_or_assign(std::move(name), std::move(module));
}

auto ExternPrelude::module(std::string_view name) const
  -> Option<std::string_view> {
  if (not modules_) {
    return name;
  }
  if (auto it = modules_->find(name); it != modules_->end()) {
    return std::string_view{it->second};
  }
  return None{};
}

auto ExternPrelude::all() const -> bool {
  return not modules_;
}

auto resolve_entities(ast::pipeline& pipe, ExternPrelude const& prelude,
                      base_ctx ctx) -> failure_or<void> {
  return resolve(pipe, prelude, ctx, ctx);
}

auto resolve_entities(ast::pipeline& pipe, ExternPrelude const& prelude,
                      session ctx) -> failure_or<void> {
  return resolve(pipe, prelude, ctx.reg(), ctx.dh());
}

auto resolve_entities(ast::expression& expr, ExternPrelude const& prelude,
                      base_ctx ctx) -> failure_or<void> {
  return resolve(expr, prelude, ctx, ctx);
}

auto resolve_entities(ast::expression& expr, ExternPrelude const& prelude,
                      session ctx) -> failure_or<void> {
  return resolve(expr, prelude, ctx.reg(), ctx.dh());
}

auto resolve_entities(ast::function_call& fc, ExternPrelude const& prelude,
                      session ctx) -> failure_or<void> {
  return resolve(fc, prelude, ctx.reg(), ctx.dh());
}

auto entities_resolved(ast::pipeline& pipe) -> bool {
  auto check = ResolutionCheck{};
  check.visit(pipe);
  return check.resolved();
}

} // namespace tenzir
