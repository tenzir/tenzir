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
#include "tenzir/tql2/parser.hpp"
#include "tenzir/tql2/registry.hpp"
#include "tenzir/tql2/resolve.hpp"
#include "tenzir/tql2/tokens.hpp"
#include "tenzir/type.hpp"

#include <arrow/util/utf8.h>

namespace tenzir::tql2 {

namespace {

using namespace ast;

class type_checker {
public:
  using result = std::optional<type>;

  explicit type_checker(context& ctx) : ctx_{ctx} {
  }

  auto visit(literal& x) -> result {
    return x.value.match(
      []<class T>(T&) -> type {
        return type{data_to_type_t<T>{}};
      },
      [](null&) -> type {
        return type{null_type{}};
      });
  }

  auto visit(selector&) -> result {
    return std::nullopt;
  }

  auto visit(expression& x) -> result {
    return x.match([&](auto& y) {
      return visit(y);
    });
  }

  auto visit(binary_expr& x) -> result {
    // TODO: This is just for testing.
    auto left = visit(x.left);
    auto right = visit(x.right);
    if (left && right && left != right) {
      diagnostic::error("cannot {} `{}` and `{}`", x.op.inner, *left, *right)
        .primary(x.op.source)
        .secondary(x.left.get_location(), "this is `{}`", *left)
        .secondary(x.right.get_location(), "this is `{}`", *right)
        .emit(ctx_.dh());
      return std::nullopt;
    }
    if (left) {
      return left;
    }
    return right;
  }

  auto visit(unary_expr& x) -> result {
    // TODO
    auto ty_opt = visit(x.expr);
    if (not ty_opt) {
      return {};
    }
    auto ty = std::move(*ty_opt);
    switch (x.op.inner) {
      case unary_op::pos:
      case unary_op::neg:
        if (ty != type{int64_type{}}) {
          diagnostic::error("cannot {} `{}`", x.op.inner, ty)
            .primary(x.op.source)
            .secondary(x.expr.get_location(), "this is `{}`", ty)
            .emit(ctx_.dh());
          return std::nullopt;
        }
        return ty;
      case unary_op::not_:
        if (ty != type{bool_type{}}) {
          diagnostic::error("cannot {} `{}`", x.op.inner, ty)
            .primary(x.op.source)
            .secondary(x.expr.get_location(), "this is `{}`", ty)
            .emit(ctx_.dh());
          return std::nullopt;
        }
        return ty;
    }
    TENZIR_UNREACHABLE();
  }

  auto visit(function_call& x) -> result {
    auto subject = std::optional<result>{};
    if (x.subject) {
      subject = visit(*x.subject);
    }
    auto args = std::vector<result>{};
    for (auto& arg : x.args) {
      args.push_back(visit(arg));
    }
    if (not x.fn.ref.resolved()) {
      return std::nullopt;
    }
    // TODO: Improve.
    auto fn
      = std::get_if<std::unique_ptr<function_def>>(&ctx_.reg().get(x.fn.ref));
    TENZIR_ASSERT(fn);
    TENZIR_ASSERT(*fn);
    auto info = function_def::check_info{x.fn.get_location(), x.args, args};
    return (*fn)->check(info, ctx_);
  }

  auto visit(assignment& x) -> result {
    visit(x.right);
    // TODO
    diagnostic::error("unexpected assignment")
      .primary(x.get_location())
      .emit(ctx_.dh());
    return std::nullopt;
  }

  auto visit(pipeline_expr& x) -> result {
    // TODO: How would this work?
    visit(x.inner);
  }

  template <class T>
  auto visit(T&) -> result {
    TENZIR_WARN("missed: {}", typeid(T).name());
    return std::nullopt;
  }

private:
  context& ctx_;
};

/// A diagnostic handler that remembers when it has emits an error.
class diagnostic_handler_wrapper final : public diagnostic_handler {
public:
  explicit diagnostic_handler_wrapper(std::unique_ptr<diagnostic_handler> inner)
    : inner_{std::move(inner)} {
  }

  void emit(diagnostic d) override {
    if (d.severity == severity::error) {
      error_ = true;
    }
    inner_->emit(std::move(d));
  }

  auto error() const -> bool {
    return error_;
  }

private:
  bool error_ = false;
  std::unique_ptr<diagnostic_handler> inner_;
};

struct sort_expr {
  enum class direction { asc, desc };

  sort_expr(expression expr, direction dir) : expr{std::move(expr)}, dir{dir} {
  }

  expression expr;
  direction dir;
};

class sort_use final : public operator_use {
public:
  explicit sort_use(entity self, std::vector<sort_expr> exprs)
    : self_{std::move(self)}, exprs_{std::move(exprs)} {
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
        [&](tql2::ast::unary_expr& un_expr) {
          if (un_expr.op.inner == tql2::ast::unary_op::neg) {
            exprs.emplace_back(std::move(un_expr.expr),
                               sort_expr::direction::desc);
          } else {
            exprs.emplace_back(std::move(arg), sort_expr::direction::asc);
          }
        },
        [&](assignment& x) {
          diagnostic::error("we don't like assignments around here")
            .primary(x.get_location())
            .emit(ctx.dh());
        },
        [&](auto&) {
          exprs.emplace_back(std::move(arg), sort_expr::direction::asc);
        });
    }
    for (auto& expr : exprs) {
      type_checker{ctx}.visit(expr.expr);
    }
    return std::make_unique<sort_use>(std::move(self), std::move(exprs));
  }
};

class from_use final : public operator_use {
public:
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
      return nullptr;
    }
    auto arg = std::move(args[0]);
    auto ty = type_checker{ctx}.visit(arg);
    if (ty && ty != string_type{}) {
      diagnostic::error("expected `string` but got `{}`", *ty)
        .primary(arg.get_location())
        .emit(ctx.dh());
      return nullptr;
    }
    // TODO: This should be some kind of const-eval, but for now, we just expect
    // a string literal.
    using result = std::optional<located<std::string>>;
    auto url = arg.match(
      [](literal& x) -> result {
        return x.value.match(
          [&](std::string& y) -> result {
            return {{y, x.source}};
          },
          [](auto&) -> result {
            return std::nullopt;
          });
      },
      [](auto&) -> result {
        return std::nullopt;
      });
    if (not url) {
      diagnostic::error("expected a string literal")
        .primary(arg.get_location())
        .emit(ctx.dh());
      return nullptr;
    }
    (void)url;
    return std::make_unique<from_use>();
  }
};

class group_def final : public operator_def {
public:
  auto make(ast::entity self, std::vector<ast::expression> args,
            context& ctx) const -> std::unique_ptr<operator_use> override {
    for (auto& arg : args) {
      type_checker{ctx}.visit(arg);
    }
    diagnostic::error("not implemented yet")
      .primary(self.get_location())
      .emit(ctx.dh());
    return nullptr;
  }
};

class sqrt_def final : public function_def {
public:
  auto check(check_info info, context& ctx) const
    -> std::optional<type> override {
    auto dbl = type{double_type{}};
    if (info.arg_count() == 0) {
      diagnostic::error("`sqrt` expects one argument")
        .primary(info.fn_loc())
        .emit(ctx.dh());
      return dbl;
    }
    auto& ty = info.arg_type(0);
    if (ty && ty != dbl) {
      // TODO: Use name of function?
      diagnostic::error("`sqrt` expected `{}` but got `{}`", dbl, *ty)
        .primary(info.arg_loc(0), "this is `{}`", *ty)
        .secondary(info.fn_loc(), "this expected `{}`", dbl)
        .emit(ctx.dh());
    }
    if (info.arg_count() > 1) {
      diagnostic::error("`sqrt` expects only one argument")
        .primary(info.arg_loc(1))
        .emit(ctx.dh());
    }
    return dbl;
  }
};

class now_def final : public function_def {
public:
  auto check(check_info info, context& ctx) const
    -> std::optional<type> override {
    if (info.arg_count() > 0) {
      diagnostic::error("`now` does not expect any arguments")
        .primary(info.arg_loc(0))
        .emit(ctx.dh());
    }
    return type{duration_type{}};
  }
};

} // namespace

auto exec(std::string content, std::unique_ptr<diagnostic_handler> diag,
          const exec_config& cfg, caf::actor_system& sys) -> bool {
  (void)sys;
  auto content_view = std::string_view{content};
  auto tokens = tql2::tokenize(content);
  auto diag_wrapper = diagnostic_handler_wrapper{std::move(diag)};
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
          .emit(diag_wrapper);
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
      has_error |= token.kind == tql2::token_kind::error;
    }
    return not has_error;
  }
  for (auto& token : tokens) {
    if (token.kind == tql2::token_kind::error) {
      auto begin = size_t{0};
      if (&token != tokens.data()) {
        begin = (&token - 1)->end;
      }
      diagnostic::error("could not parse token")
        .primary(location{begin, token.end})
        .emit(diag_wrapper);
    }
  }
  if (diag_wrapper.error()) {
    return false;
  }
  auto parsed = tql2::parse(tokens, content, diag_wrapper);
  if (diag_wrapper.error()) {
    return false;
  }
  auto reg = registry{};
  reg.add("sort", std::make_unique<sort_def>());
  reg.add("from", std::make_unique<from_def>());
  reg.add("group", std::make_unique<group_def>());
  reg.add("sqrt", std::make_unique<sqrt_def>());
  reg.add("now", std::make_unique<now_def>());
  tql2::resolve_entities(parsed, reg, diag_wrapper);
  if (cfg.dump_ast) {
    with_thread_local_registry(reg, [&] {
      fmt::println("{:#?}", parsed);
    });
    return not diag_wrapper.error();
  }
  // TODO
  auto ops = std::vector<std::unique_ptr<operator_use>>{};
  auto ctx = context{reg, diag_wrapper};
  for (auto& stmt : parsed.body) {
    // assignment, let_stmt, if_stmt, match_stmt
    stmt.match(
      [&](invocation& x) {
        if (not x.op.ref.resolved()) {
          // This was already reported. We don't know how the operator would
          // interpret its arguments, hence we make no attempt of reporting
          // additional errors for them.
          return;
        }
        // TODO: Where do we check that this succeeds?
        auto def
          = std::get_if<std::unique_ptr<operator_def>>(&reg.get(x.op.ref));
        TENZIR_ASSERT(def);
        TENZIR_ASSERT(*def);
        auto use = (*def)->make(x.op, std::move(x.args), ctx);
        if (use) {
          ops.push_back(std::move(use));
        } else {
          TENZIR_ASSERT(diag_wrapper.error());
        }
      },
      [&](assignment& x) {
        // TODO: This is just for testing.
        class test final : public operator_use {
        public:
          explicit test(assignment x) : x_{std::move(x)} {
          }

        private:
          assignment x_;
        };
        auto ty = type_checker{ctx}.visit(x.right);
        if (x.left.path.empty()) {
          TENZIR_ASSERT(x.left.this_);
          if (ty && *ty != type{record_type{}}) {
            diagnostic::error("only records can be assigned to `this`")
              .primary(x.right.get_location(), "this is `{}`", *ty)
              .emit(ctx.dh());
          }
        }
        ops.push_back(std::make_unique<test>(std::move(x)));
      },
      [&](auto& x) {
        diagnostic::error("unimplemented: {}", typeid(x).name())
          .emit(diag_wrapper);
      });
  }
  if (diag_wrapper.error()) {
    return false;
  }
  diagnostic::warning(
    "pipeline is syntactically valid, but execution is not yet implemented")
    .hint("use `--dump-ast` to show AST")
    .emit(diag_wrapper);
  return true;
}

} // namespace tenzir::tql2
