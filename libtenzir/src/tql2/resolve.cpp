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
#include <ranges>
#include <unordered_set>

namespace tenzir {

namespace {

class entity_resolver : public ast::visitor<entity_resolver> {
public:
  explicit entity_resolver(const registry& reg, diagnostic_handler& diag)
    : reg_{reg}, diag_{diag} {
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
      auto suggestion = std::optional<std::string>{};
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
    // Because there currently is no way to bring additional entities into the
    // scope, we can directly dispatch to the registry.
    auto pkg = std::invoke([&]() -> std::optional<entity_pkg> {
      // Resolution precedence: cfg (user overrides) -> std (builtins) ->
      // packages (installed)
      for (auto pkg : {std::string{entity_pkg_cfg}, std::string{entity_pkg_std},
                       std::string{"packages"}}) {
        auto path = entity_path{pkg, {x.path[0].name}, first_ns};
        if (is<entity_ref>(reg_.try_get(path))) {
          return pkg;
        }
      }
      return std::nullopt;
    });
    if (not pkg) {
      report_not_found(x.path, 0, first_ns);
      return;
    }
    // After figuring out which package it belongs to, we try the full path.
    auto segments = std::vector<std::string>{};
    segments.reserve(x.path.size());
    for (auto& segment : x.path) {
      segments.push_back(segment.name);
    };
    auto path = entity_path{*pkg, std::move(segments), target_ns};
    auto result = reg_.try_get(path);
    auto err = try_as<registry::error>(result);
    if (err) {
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
    // The full path is the module segments followed by the binding name.
    auto segments = std::vector<std::string>{};
    segments.reserve(x.path.size() + 1);
    for (auto& segment : x.path) {
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
    auto path = entity_path{std::string{"packages"}, segments, entity_ns::let};
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
    // available, then cache the constant result on the node.
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
    // merely warns and yields `null`. Stricter structural checking is deferred
    // to a later change.
    auto value = const_eval(expr, diag_);
    if (not value) {
      result_ = failure::promise();
      return;
    }
    x.ref = std::move(path);
    x.value = std::move(value).unwrap();
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
  enum class context_t { none, op_name, fn_name };
  const registry& reg_;
  diagnostic_handler& diag_;
  context_t context_ = context_t::none;
  failure_or<void> result_;
  // Package `let` paths currently being resolved, to detect reference cycles.
  std::unordered_set<std::string> resolving_;
};

} // namespace

auto resolve_entities(ast::pipeline& pipe, session ctx) -> failure_or<void> {
  auto resolver = entity_resolver{ctx.reg(), ctx.dh()};
  resolver.visit(pipe);
  return resolver.get_failure();
}

auto resolve_entities(ast::pipeline& pipe, base_ctx ctx) -> failure_or<void> {
  auto resolver = entity_resolver{ctx, ctx};
  resolver.visit(pipe);
  return resolver.get_failure();
}

auto resolve_entities(ast::expression& expr, session ctx) -> failure_or<void> {
  auto resolver = entity_resolver{ctx.reg(), ctx.dh()};
  resolver.visit(expr);
  return resolver.get_failure();
}

auto resolve_entities(ast::function_call& fc, session ctx) -> failure_or<void> {
  auto resolver = entity_resolver{ctx.reg(), ctx.dh()};
  resolver.visit(fc);
  return resolver.get_failure();
}

} // namespace tenzir
