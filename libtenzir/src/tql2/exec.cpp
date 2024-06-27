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

#include <arrow/util/utf8.h>
#include <boost/functional/hash.hpp>
#include <tsl/robin_set.h>

namespace tenzir {
namespace {

/// A diagnostic handler that remembers when it emits an error, and also
/// does not emit the same diagnostic twice.
class diagnostic_handler_wrapper final : public diagnostic_handler {
public:
  explicit diagnostic_handler_wrapper(std::unique_ptr<diagnostic_handler> inner)
    : inner_{std::move(inner)} {
  }

  void emit(diagnostic d) override {
    // We remember whether we have seen a diagnostic by storing its main message
    // and the locations of its annotations.
    // TODO: Improve this.
    auto locations = std::vector<location>{};
    for (auto& annotation : d.annotations) {
      locations.push_back(annotation.source);
    }
    auto inserted
      = seen_.emplace(std::pair{d.message, std::move(locations)}).second;
    if (not inserted) {
      return;
    }
    if (d.severity == severity::error) {
      error_ = true;
    }
    inner_->emit(std::move(d));
  }

  auto error() const -> bool {
    return error_;
  }

  auto inner() && -> std::unique_ptr<diagnostic_handler> {
    return std::move(inner_);
  }

private:
  using seen_t = std::pair<std::string, std::vector<location>>;

  struct hasher {
    auto operator()(const seen_t& x) const -> size_t {
      auto result = std::hash<std::string>{}(x.first);
      for (auto& loc : x.second) {
        boost::hash_combine(result, loc.begin);
        boost::hash_combine(result, loc.end);
      }
      return result;
    }
  };

  bool error_ = false;
  tsl::robin_set<seen_t, hasher> seen_;
  std::unique_ptr<diagnostic_handler> inner_;
};

} // namespace

auto prepare_pipeline(ast::pipeline&& pipe, session ctx) -> pipeline {
  auto ops = std::vector<operator_ptr>{};
  for (auto& stmt : pipe.body) {
    stmt.match(
      [&](ast::invocation& x) {
        if (not x.op.ref.resolved()) {
          // This was already reported. We don't know how the operator would
          // interpret its arguments, hence we make no attempt of reporting
          // additional errors for them.
          return;
        }
        // TODO: Where do we check that this succeeds?
        auto def = std::get_if<const operator_factory_plugin*>(
          &ctx.reg().get(x.op.ref));
        TENZIR_ASSERT(def);
        TENZIR_ASSERT(*def);
        auto op = (*def)->make(
          operator_factory_plugin::invocation{
            std::move(x.op),
            std::move(x.args),
          },
          ctx);
        if (op) {
          ops.push_back(std::move(op));
        } else {
          // We assume we emitted an error.
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
        ops.push_back(std::move(op));
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
        ops.push_back(std::move(op));
      },
      [&](ast::match_stmt& x) {
        diagnostic::error("`match` not yet implemented, try using `if` instead")
          .primary(x)
          .emit(ctx.dh());
      },
      [&](ast::let_stmt& x) {
        diagnostic::error("`let` statements are not implemented yet")
          .primary(x)
          .emit(ctx);
      });
  }
  return tenzir::pipeline{std::move(ops)};
}

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

private:
  std::unordered_map<std::string, std::optional<ast::constant::kind>> map_;
  session ctx_;
};

void resolve_let_statements(ast::pipeline& pipe, session ctx) {
  let_resolver{ctx}.visit(pipe);
}

auto exec2(std::string content, std::unique_ptr<diagnostic_handler> diag,
           const exec_config& cfg, caf::actor_system& sys) -> bool {
  (void)sys;
  auto content_view = std::string_view{content};
  auto tokens = tokenize(content);
  auto diag_wrapper
    = std::make_unique<diagnostic_handler_wrapper>(std::move(diag));
  // TODO: Refactor this.
  arrow::util::InitializeUTF8();
  if (not arrow::util::ValidateUTF8(content)) {
    // Figure out the exact token.
    auto last = size_t{0};
    for (auto& token : tokens) {
      if (not arrow::util::ValidateUTF8(content_view.substr(last, token.end))) {
        // TODO: We can't really do this directly, unless we handle invalid
        // UTF-8 in diagnostics.
        diagnostic::error("invalid UTF8")
          .primary(location{last, token.end})
          .emit(*diag_wrapper);
      }
      last = token.end;
    }
    return false;
  }
  if (cfg.dump_tokens) {
    auto last = size_t{0};
    auto has_error = false;
    for (auto& token : tokens) {
      fmt::print("{:>15} {:?}\n", token.kind,
                 content_view.substr(last, token.end - last));
      last = token.end;
      has_error |= token.kind == token_kind::error;
    }
    return not has_error;
  }
  for (auto& token : tokens) {
    if (token.kind == token_kind::error) {
      auto begin = size_t{0};
      if (&token != tokens.data()) {
        begin = (&token - 1)->end;
      }
      diagnostic::error("could not parse token")
        .primary(location{begin, token.end})
        .emit(*diag_wrapper);
    }
  }
  if (diag_wrapper->error()) {
    return false;
  }
  auto parsed = parse(tokens, content, *diag_wrapper);
  if (diag_wrapper->error()) {
    return false;
  }
  if (cfg.dump_ast) {
    fmt::print("{:#?}\n", parsed);
    return not diag_wrapper->error();
  }
  auto ctx = session{*diag_wrapper};
  resolve_entities(parsed, ctx);
  // TODO: Can we proceed with unresolved entities?
  if (diag_wrapper->error()) {
    return false;
  }
  resolve_let_statements(parsed, ctx);
  if (diag_wrapper->error()) {
    return false;
  }
  auto pipe = prepare_pipeline(std::move(parsed), ctx);
  if (diag_wrapper->error()) {
    return false;
  }
  // TODO: Reduce diagnostic handler wrapping.
  auto result
    = exec_pipeline(std::move(pipe),
                    std::make_unique<diagnostic_handler_ref>(*diag_wrapper),
                    cfg, sys);
  if (not result) {
    diagnostic::error(result.error()).emit(*diag_wrapper);
    return false;
  }
  return true;
}

} // namespace tenzir
