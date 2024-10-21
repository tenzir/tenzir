//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/exec.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/exec_pipeline.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/session.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/parser.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/tql2/registry.hpp"
#include "tenzir/tql2/resolve.hpp"
#include "tenzir/tql2/tokens.hpp"
#include "tenzir/try.hpp"
#include "tenzir/variant_traits.hpp"

#include <arrow/util/utf8.h>
#include <tsl/robin_set.h>

namespace tenzir {
namespace {

// TODO: This is a naive implementation and does not do scoping properly.
class let_resolver : public ast::visitor<let_resolver> {
public:
  explicit let_resolver(session ctx) : ctx_{ctx} {
  }

  void visit(ast::pipeline& x) {
    // TODO: Extraction + patch is probably a common pattern.
    for (auto it = x.body.begin(); it != x.body.end();) {
      auto let = std::get_if<ast::let_stmt>(&*it);
      if (not let) {
        visit(*it);
        ++it;
        continue;
      }
      visit(let->expr);
      auto value = const_eval(let->expr, ctx_);
      if (value) {
        map_[let->name.name] = tenzir::match(
          std::move(*value),
          [](auto x) -> ast::constant::kind {
            return x;
          },
          [](const pattern&) -> ast::constant::kind {
            // TODO
            TENZIR_UNREACHABLE();
          });
      } else {
        map_[let->name.name] = std::nullopt;
      }
      it = x.body.erase(it);
    }
  }

  void visit(ast::expression& x) {
    auto dollar_var = std::get_if<ast::dollar_var>(&*x.kind);
    if (not dollar_var) {
      enter(x);
      return;
    }
    auto it = map_.find(dollar_var->name);
    if (it == map_.end()) {
      diagnostic::error("unresolved variable").primary(x).emit(ctx_);
      return;
    }
    if (not it->second) {
      // Variable exists but there was an error during evaluation.
      return;
    }
    x = ast::constant{*it->second, x.get_location()};
  }

  template <class T>
  void visit(T& x) {
    enter(x);
  }

  auto get_failure() -> failure_or<void> {
    return failure_;
  }

private:
  failure_or<void> failure_;
  std::unordered_map<std::string, std::optional<ast::constant::kind>> map_;
  session ctx_;
};

auto resolve_let_bindings(ast::pipeline& pipe, session ctx)
  -> failure_or<void> {
  auto resolver = let_resolver{ctx};
  resolver.visit(pipe);
  return resolver.get_failure();
}

auto compile_resolved(ast::pipeline&& pipe, session ctx)
  -> failure_or<pipeline> {
  auto fail = std::optional<failure>{};
  auto ops = std::vector<operator_ptr>{};
  for (auto& stmt : pipe.body) {
    stmt.match(
      [&](ast::invocation& x) {
        // TODO: Where do we check that this succeeds?
        auto op = ctx.reg().get(x).make(
          operator_factory_plugin::invocation{
            std::move(x.op),
            std::move(x.args),
          },
          ctx);
        if (op) {
          TENZIR_ASSERT(*op);
          ops.push_back(std::move(*op));
        } else {
          fail = op.error();
        }
      },
      [&](ast::assignment& x) {
#if 0
        // TODO: Cannot do this right now (release typeid problem).
        auto assignments = std::vector<assignment>();
        assignments.push_back(std::move(x));
        ops.push_back(std::make_unique<set_operator>(std::move(assignments)));
#else
        auto plugin = plugins::find<operator_factory_plugin>("tql2.set");
        TENZIR_ASSERT(plugin);
        auto args = std::vector<ast::expression>{};
        args.emplace_back(std::move(x));
        auto op = plugin->make(
          operator_factory_plugin::invocation{
            ast::entity{
              {ast::identifier{std::string{"set"}, location::unknown}}},
            std::move(args),
          },
          ctx);
        TENZIR_ASSERT(op);
        ops.push_back(std::move(*op));
#endif
      },
      [&](ast::if_stmt& x) {
        // TODO: Same problem regarding instantiation outside of plugin.
        auto args = std::vector<ast::expression>{};
        args.reserve(3);
        args.push_back(std::move(x.condition));
        args.emplace_back(ast::pipeline_expr{
          location::unknown, std::move(x.then), location::unknown});
        if (x.else_) {
          args.emplace_back(ast::pipeline_expr{
            location::unknown, std::move(*x.else_), location::unknown});
        }
        auto plugin = plugins::find<operator_factory_plugin>("tql2.if");
        TENZIR_ASSERT(plugin);
        auto op = plugin->make(
          operator_factory_plugin::invocation{
            ast::entity{{ast::identifier{std::string{"if"}, location::unknown}}},
            std::move(args),
          },
          ctx);
        TENZIR_ASSERT(op);
        ops.push_back(std::move(*op));
      },
      [&](ast::match_stmt& x) {
        diagnostic::error("`match` not yet implemented, try using `if` instead")
          .primary(x)
          .emit(ctx.dh());
        fail = failure::promise();
      },
      [&](ast::let_stmt&) {
        TENZIR_UNREACHABLE();
      });
  }
  if (fail) {
    return *fail;
  }
  return tenzir::pipeline{std::move(ops)};
}

} // namespace

auto parse_and_compile(std::string_view source, session ctx)
  -> failure_or<pipeline> {
  TRY(auto ast, parse(source, ctx));
  return compile(std::move(ast), ctx);
}

auto compile(ast::pipeline&& pipe, session ctx) -> failure_or<pipeline> {
  TRY(resolve_entities(pipe, ctx));
  TRY(resolve_let_bindings(pipe, ctx));
  return compile_resolved(std::move(pipe), ctx);
}

auto dump_tokens(std::span<token const> tokens, std::string_view source)
  -> bool {
  auto last = size_t{0};
  auto has_error = false;
  for (auto& token : tokens) {
    fmt::print("{:>15} {:?}\n", token.kind,
               source.substr(last, token.end - last));
    last = token.end;
    has_error |= token.kind == token_kind::error;
  }
  return not has_error;
}

auto exec2(std::string_view source, diagnostic_handler& dh,
           const exec_config& cfg, caf::actor_system& sys) -> bool {
  TENZIR_UNUSED(sys);
  auto result = std::invoke([&]() -> failure_or<bool> {
    auto provider = session_provider::make(dh);
    auto ctx = provider.as_session();
    TRY(validate_utf8(source, ctx));
    auto tokens = tokenize_permissive(source);
    if (cfg.dump_tokens) {
      return dump_tokens(tokens, source);
    }
    TRY(verify_tokens(tokens, ctx));
    TRY(auto parsed, parse(tokens, source, ctx));
    if (cfg.dump_ast) {
      fmt::print("{:#?}\n", parsed);
      return not ctx.has_failure();
    }
    TRY(auto pipe, compile(std::move(parsed), ctx));
    if (cfg.dump_pipeline) {
      fmt::print("{:#?}\n", pipe);
      return not ctx.has_failure();
    }
    if (ctx.has_failure()) {
      // Do not proceed to execution if there has been an error.
      return false;
    }
    auto result = exec_pipeline(std::move(pipe), ctx, cfg, sys);
    if (not result) {
      diagnostic::error(result.error()).emit(ctx);
    }
    return not ctx.has_failure();
  });
  return result ? *result : false;
}

} // namespace tenzir
