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
        auto f = detail::overload{
          [](auto x) -> ast::constant::kind {
            return x;
          },
          [](pattern&) -> ast::constant::kind {
            // TODO
            TENZIR_UNREACHABLE();
          },
        };
        map_[let->name.name] = caf::visit(f, std::move(*value));
      } else {
        map_[let->name.name] = std::nullopt;
      }
      it = x.body.erase(it);
    }
  }

  void emit_not_found(const ast::dollar_var& var) {
    diagnostic::error("variable `{}` was not declared", var.name)
      .primary(var)
      .emit(ctx_);
    failure_ = failure::promise();
  }

  void visit(ast::expression& x) {
    auto dollar_var = std::get_if<ast::dollar_var>(&*x.kind);
    if (not dollar_var) {
      enter(x);
      return;
    }
    auto it = map_.find(dollar_var->name);
    if (it == map_.end()) {
      emit_not_found(*dollar_var);
      return;
    }
    if (not it->second) {
      // Variable exists but there was an error during evaluation.
      return;
    }
    x = ast::constant{*it->second, x.get_location()};
  }

  void load_balance(ast::invocation& x) {
    // TODO: Remove special casing.
    auto docs = "https://docs.tenzir.com/tql2/operators/load_balance";
    auto usage = "load_balance over:list { … }";
    auto emit = [&](diagnostic_builder d) {
      if (d.inner().severity == severity::error) {
        failure_ = failure::promise();
      }
      std::move(d).docs(docs).usage(usage).emit(ctx_);
    };
    // Remove all the arguments, as we will be replacing them anyway.
    auto args = std::move(x.args);
    x.args.clear();
    if (args.empty()) {
      emit(
        diagnostic::error("expected two positional arguments").primary(x.op));
      return;
    }
    auto var = std::get_if<ast::dollar_var>(&*args[0].kind);
    if (not var) {
      emit(diagnostic::error("expected a `$`-variable").primary(args[0]));
      return;
    }
    if (args.size() < 2) {
      emit(diagnostic::error("expected a pipeline afterwards").primary(*var));
      return;
    }
    auto it = map_.find(var->name);
    if (it == map_.end()) {
      emit_not_found(*var);
      return;
    }
    if (not it->second) {
      // Variable exists, but there was an error during evaluation.
      return;
    }
    auto pipe = std::get_if<ast::pipeline_expr>(&*args[1].kind);
    if (not pipe) {
      emit(
        diagnostic::error("expected a pipeline expression").primary(args[1]));
      return;
    }
    // We now expand the pipeline once for each entry in the list, replacing the
    // original variable with the list items.
    auto original = std::move(*it->second);
    auto entries = std::get_if<list>(&original);
    if (not entries) {
      auto got = original.match([]<class T>(const T&) {
        return type_kind::of<data_to_type_t<T>>;
      });
      emit(diagnostic::error("expected a list, got `{}`", got).primary(*var));
      *it->second = std::move(original);
      return;
    }
    for (auto& entry : *entries) {
      auto f = detail::overload{
        [](const auto& x) -> ast::constant::kind {
          return x;
        },
        [](const pattern&) -> ast::constant::kind {
          TENZIR_UNREACHABLE();
        },
      };
      auto constant = caf::visit(f, entry);
      map_.insert_or_assign(var->name, constant);
      auto pipe_copy = *pipe;
      visit(pipe_copy);
      x.args.emplace_back(std::move(pipe_copy));
    }
    if (args.size() > 2) {
      emit(
        diagnostic::error("expected exactly two arguments, got {}", args.size())
          .primary(args[2]));
    }
    // Restore the original value in case it's used elsewhere.
    map_.insert_or_assign(var->name, std::move(original));
  }

  void visit(ast::invocation& x) {
    if (x.op.ref.resolved() and x.op.ref.segments().size() == 1
        and x.op.ref.segments()[0] == "load_balance") {
      // We special case this as a temporary solution.
      load_balance(x);
      return;
    }
    enter(x);
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
