//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/resolve.hpp"

#include "tenzir/compile_ctx.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/similarity.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/registry.hpp"

#include <tsl/robin_map.h>

#include <ranges>
#include <tenzir/logger.hpp>

#undef TENZIR_DEBUG
#define TENZIR_DEBUG TENZIR_INFO

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
    TENZIR_DEBUG("resolver: visiting entity with raw path '{}'", fmt::join(
                   std::views::transform(x.path, &ast::identifier::name),
                   "::"));
    // We use the following logic:
    // - Look at the first segment.
    // - Use its name + namespace (mod/fn/op) for lookup as user-defined.
    // - If something is found: Use rest of the segments. If not found: Error.
    // - If nothing is found: Repeat with lookup for built-in entities.
    const auto target_ns
      = context_ == context_t::op_name ? entity_ns::op : entity_ns::fn;
    const auto first_ns = x.path.size() == 1 ? target_ns : entity_ns::mod;
    TENZIR_DEBUG("resolver: target_ns={}, first_ns={}", target_ns, first_ns);
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
      if (available.empty()) {
        diagnostic::error("{} `{}` not found", type, ident.name)
          .primary(ident)
          .note("no {}s found", type)
          .emit(diag_);
        return;
      }
      auto filtered = std::views::filter(available, [&](auto&& x) {
        return x.starts_with(prefix);
      });
      if (not filtered.empty()) {
        auto best = std::ranges::max(filtered, {}, [&](auto&& x) {
          return detail::calculate_similarity(full, x);
        });
        if (const auto pos = best.find(prefix);
            not prefix.empty() and pos == 0) {
          best.erase(0, prefix.size() + 2);
        }
        if (detail::calculate_similarity(full, best) > -5) {
          diagnostic::error("{} `{}` not found", type, ident.name)
            .primary(ident)
            .hint("did you mean `{}`?", best)
            .docs("https://docs.tenzir.com/reference/{}",
                  target_ns == entity_ns::op ? "operators" : "functions")
            .emit(diag_);
          return;
        }
      }
      diagnostic::error("{} `{}` not found", type, ident.name)
        .primary(ident)
        .docs("https://docs.tenzir.com/reference/{}",
              target_ns == entity_ns::op ? "operators" : "functions")
        .emit(diag_);
    };
    // Because there currently is no way to bring additional entities into the
    // scope, we can directly dispatch to the registry.
    auto pkg = std::invoke([&]() -> std::optional<entity_pkg> {
      // Resolution precedence: cfg (user overrides) -> std (builtins) ->
      // packages (installed)
      for (auto pkg : {std::string{entity_pkg_cfg}, std::string{entity_pkg_std},
                       std::string{"packages"}}) {
        auto path = entity_path{pkg, {x.path[0].name}, first_ns};
        TENZIR_DEBUG("resolver: probing root='{}' segment='{}' ns={}",
                     pkg, x.path[0].name, first_ns);
        if (is<entity_ref>(reg_.try_get(path))) {
          TENZIR_DEBUG("resolver: found root='{}' for first segment '{}'", pkg,
                       x.path[0].name);
          return pkg;
        }
        TENZIR_DEBUG("resolver: not found in root '{}'", pkg);
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
    TENZIR_DEBUG("resolver: probing full path='{}::{}/{}'", *pkg,
                 fmt::join(path.segments(), "::"), target_ns);
    auto result = reg_.try_get(path);
    auto err = try_as<registry::error>(result);
    if (err) {
      TENZIR_DEBUG("resolver: failed to resolve at segment {} (other_exists={})",
                   err->segment, err->other_exists);
      TENZIR_ASSERT(err->segment < x.path.size());
      auto is_last = err->segment == path.segments().size() - 1;
      auto error_ns = is_last ? target_ns : entity_ns::mod;
      report_not_found(x.path, err->segment, error_ns);
      return;
    }
    TENZIR_DEBUG("resolver: success resolving entity");
    x.ref = std::move(path);
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
