//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/exec.hpp"

#include "tenzir/bitmap.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/exec_pipeline.hpp"
#include "tenzir/hash/hash.hpp"
#include "tenzir/pipeline.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/session.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/check_type.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/parser.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/tql2/registry.hpp"
#include "tenzir/tql2/resolve.hpp"
#include "tenzir/tql2/set.hpp"
#include "tenzir/tql2/tokens.hpp"
#include "tenzir/type.hpp"

#include <arrow/compute/api.h>
#include <arrow/util/utf8.h>
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
    // TODO: This is obviously quite bad great.
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
      // TODO: Very, very, very bad!!
      auto result = std::hash<std::string>{}(x.first);
      for (auto& loc : x.second) {
        result ^= loc.begin << 1 ^ loc.end;
      }
      return result;
    }
  };

  bool error_ = false;
  tsl::robin_set<seen_t, hasher> seen_;
  std::unique_ptr<diagnostic_handler> inner_;
};

#if 0
TENZIR_ENUM(sort_direction, asc, desc);

struct sort_expr {
  sort_expr(expression expr, sort_direction dir)
    : expr{std::move(expr)}, dir{dir} {
  }

  expression expr;
  sort_direction dir;

  friend auto inspect(auto& f, sort_expr& x) -> bool {
    return f.object(x).fields(f.field("expr", x.expr), f.field("dir", x.dir));
  }
};

class sort_use final : public operator_use {
public:
  explicit sort_use(entity self, std::vector<sort_expr> exprs)
    : self_{std::move(self)}, exprs_{std::move(exprs)} {
  }

  // void, byte, chunk, event
  // void, chunk_ptr, vector<chunk_ptr>, table_slice

  auto debug(debug_writer& f) -> bool override {
    return f.object(*this).fields(f.field("self", self_),
                                  f.field("exprs", exprs_));
  }

private:
  entity self_;
  std::vector<sort_expr> exprs_;
};

class sort_def final : public operator_def {
public:
  auto make(ast::entity self, std::vector<expression> args, context& ctx) const
    -> std::unique_ptr<operator_use> override {
    auto exprs = std::vector<sort_expr>{};
    exprs.reserve(args.size());
    for (auto& arg : args) {
      arg.match(
        [&](ast::unary_expr& un_expr) {
          if (un_expr.op.inner == ast::unary_op::neg) {
            exprs.emplace_back(std::move(un_expr.expr), sort_direction::desc);
          } else {
            exprs.emplace_back(std::move(arg), sort_direction::asc);
          }
        },
        [&](assignment& x) {
          diagnostic::error("we don't like assignments around here")
            .primary(x.get_location())
            .emit(ctx.dh());
        },
        [&](auto&) {
          exprs.emplace_back(std::move(arg), sort_direction::asc);
        });
    }
    for (auto& expr : exprs) {
      type_checker{ctx}.visit(expr.expr);
    }
    return std::make_unique<sort_use>(std::move(self), std::move(exprs));
  }
};

class plugin_operator_def : public operator_def {
public:
  explicit plugin_operator_def(const operator_factory_plugin* plugin)
    : plugin_{plugin} {
  }

  auto make(ast::entity self, std::vector<expression> args, context& ctx) const
    -> std::unique_ptr<operator_use> override {
    TENZIR_UNUSED(args);
    diagnostic::error("todo: plugin_operator_def")
      .primary(self.get_location())
      .emit(ctx);
    return nullptr;
  }

private:
  const operator_factory_plugin* plugin_;
};

class from_use final : public operator_use {
public:
  explicit from_use(located<std::string> url) {
    TENZIR_UNUSED(url);
  }
};

class from_def final : public operator_def {
public:
  auto make(ast::entity self, std::vector<ast::expression> args,
            context& ctx) const -> std::unique_ptr<operator_use> override {
    (void)self;
    if (args.size() != 1) {
      diagnostic::error("`from` expects exactly one argument")
        .primary(args.empty() ? self.get_location() : args[1].get_location())
        .emit(ctx.dh());
      if (args.empty()) {
        return nullptr;
      }
    }
    auto arg = std::move(args[0]);
    // TODO: We don't need this, do we?
    // auto ty = type_checker{ctx}.visit(arg);
    // if (ty && ty != string_type{}) {
    //   diagnostic::error("expected `string` but got `{}`", *ty)
    //     .primary(arg.get_location())
    //     .emit(ctx.dh());
    //   return nullptr;
    // }
    auto url_data = evaluate(arg, ctx);
    if (not url_data) {
      return nullptr;
    }
    auto url = caf::get_if<std::string>(&*url_data);
    if (not url) {
      diagnostic::error("expected a string")
        .primary(arg.get_location())
        .emit(ctx.dh());
      return nullptr;
    }
    return std::make_unique<from_use>(
      located{std::move(*url), arg.get_location()});
  }
};

class load_file_use final : public operator_use {
public:
  explicit load_file_use(std::optional<ast::expression> path)
    : path_{std::move(path)} {
  }

private:
  std::optional<ast::expression> path_;
};

class load_file_def final : public operator_def {
public:
  auto make(ast::entity self, std::vector<ast::expression> args,
            context& ctx) const -> std::unique_ptr<operator_use> override {
    (void)self;
    (void)args;
    auto usage = "load_file path, follow=false, mmap=false, timeout=null";
    auto docs = "https://docs.tenzir.com/operators/load_file";
    if (args.empty()) {
      diagnostic::error("expected at least one argument")
        .primary(self.get_location())
        .usage(usage)
        .docs(docs)
        .emit(ctx.dh());
    }
    // assume we want `"foo.json"` and `path="foo.json"`.
    auto path = std::optional<ast::expression>{};
    for (auto& arg : args) {
      arg.match(
        [&](assignment& x) {
          auto ty = type_checker{ctx}.visit(x.right);
          if (not x.left.this_ && x.left.path.size() == 1
              && x.left.path[0].name == "path") {
            if (ty && ty != type{string_type{}}) {
              diagnostic::error("expected `string` but got `{}`", *ty)
                .primary(x.right.get_location())
                .usage(usage)
                .docs(docs)
                .emit(ctx.dh());
            }
            path = std::move(x.right);
          } else {
            diagnostic::error("unknown named argument")
              .primary(x.left.get_location())
              .usage(usage)
              .docs(docs)
              .emit(ctx.dh());
          }
        },
        [&](auto& x) {
          auto ty = type_checker{ctx}.visit(x);
          if (path) {
            diagnostic::error("path was already specified")
              .secondary(path->get_location(), "previous definition")
              .primary(x.get_location(), "new definition")
              .usage(usage)
              .docs(docs)
              .emit(ctx.dh());
            return;
          }
          if (ty && ty != type{string_type{}}) {
            diagnostic::error("expected `string`, got `{}`", *ty)
              .primary(x.get_location())
              .usage(usage)
              .docs(docs)
              .emit(ctx.dh());
          }
          path = std::move(x);
        });
    };
    return std::make_unique<load_file_use>(std::move(path));
  }
};

struct pipeline_use {
  pipeline_use() = default;

  explicit pipeline_use(std::vector<std::unique_ptr<operator_use>> ops)
    : ops{std::move(ops)} {
  }

  std::vector<std::unique_ptr<operator_use>> ops;

  friend auto inspect(auto& f, pipeline_use& x) -> bool {
    if (not f.begin_sequence(x.ops.size())) {
      return false;
    }
    for (auto& op : x.ops) {
      if (not f.apply(*op)) {
        return false;
      }
    }
    return f.end_sequence();
  }
};

class if_use final : public operator_use {
public:
  explicit if_use(expression condition, pipeline_use then,
                  std::optional<pipeline_use> else_)
    : condition_{std::move(condition)},
      then_{std::move(then)},
      else_{std::move(else_)} {
  }

private:
  expression condition_;
  pipeline_use then_;
  std::optional<pipeline_use> else_;
};

class head_use final : public operator_use {
public:
  explicit head_use(uint64_t count) : count_{count} {
  }

private:
  uint64_t count_;
};

class head_def final : public operator_def {
public:
  auto make(ast::entity self, std::vector<ast::expression> args,
            context& ctx) const -> std::unique_ptr<operator_use> override {
    if (args.empty()) {
      diagnostic::error("expected number")
        .primary(self.get_location())
        .emit(ctx.dh());
      return nullptr;
    }
    if (args.size() > 1) {
      diagnostic::error("expected only one argument")
        .primary(args[1].get_location())
        .emit(ctx.dh());
    }
    // TODO: We want to have better errors here.
    auto value = evaluate(args[0], ctx);
    if (not value) {
      return nullptr;
    }
    // TODO: Better promotion?
    auto count = caf::get_if<int64_t>(&*value);
    if (not count) {
      diagnostic::error("expected integer")
        .primary(args[0].get_location())
        .emit(ctx.dh());
      return nullptr;
    }
    if (*count < 0) {
      diagnostic::error("expected count to be non-negative but got {}", *count)
        .primary(args[0].get_location())
        .emit(ctx.dh());
      return nullptr;
    }
    return std::make_unique<head_use>(detail::narrow<uint64_t>(*count));
  }
};

#endif

#if 1
// evaluation model !?!

// data
// arrow::Array
// bitmap (?) <-- probably not as function input?

// maybe input a `ids` for which we care about the output?
// or for which we want to `&&` the result?

// if evaluation "fails" -> use `null` for now (for whole expression??)
// and emit diagnostic

struct eval_input {
  location fn;
  std::span<located<variant<data, std::shared_ptr<arrow::Array>, bitmap>>> args;
};

#endif
} // namespace

auto prepare_pipeline(ast::pipeline&& pipe, session ctx) -> pipeline {
  auto ops = std::vector<operator_ptr>{};
  for (auto& stmt : pipe.body) {
    // let_stmt, if_stmt, match_stmt
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
        // auto ty = check_type(x.condition, ctx);
        // if (ty && *ty != type{bool_type{}}) {
        //   diagnostic::error("condition type must be `bool`, but is `{}`", *ty)
        //     .primary(x.condition.get_location())
        //     .emit(ctx.dh());
        // }
        // TODO: Same problem. Very, very hacky!
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
      },
      [&](auto& x) {
        diagnostic::error("statement not implemented yet")
          .primary(x)
          .emit(ctx.dh());
      });
  }
  return tenzir::pipeline{std::move(ops)};
}

// TODO: This does not properly handle sub-pipelines.
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
  // TODO: Todo below might be inaccurate.
  // TODO: While we want to be able to operator definitions in plugins, we do
  // not want the mere *existence* to be dependant on which plugins are loaded.
  // Instead, we should always register all operators and then emit an helpful
  // error if the corresponding plugin is not loaded.
  if (cfg.dump_ast) {
    // TODO: Registry?
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
