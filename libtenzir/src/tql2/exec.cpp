//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/exec.hpp"

#include <tenzir/tql2/parser.hpp>
#include <tenzir/tql2/registry.hpp>
#include <tenzir/tql2/resolve.hpp>
#include <tenzir/tql2/tokens.hpp>

#include <arrow/util/utf8.h>

namespace tenzir::tql2 {

using namespace ast;

struct sort_expr {
  enum class direction { asc, desc };

  sort_expr(expression expr, direction dir) : expr{std::move(expr)}, dir{dir} {
  }

  expression expr;
  direction dir;
};

class sort_use final : public operator_use {
public:
  explicit sort_use(std::vector<sort_expr> exprs) : exprs_{std::move(exprs)} {
  }

private:
  std::vector<sort_expr> exprs_;
};

class sort_definition final : public operator_def {
public:
  auto name() const -> std::string_view override {
    return "std'sort";
  }

  auto make(std::vector<expression> args)
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
            exprs.emplace_back(std::move(un_expr.expr),
                               sort_expr::direction::asc);
          }
        },
        [&](auto&) {
          exprs.emplace_back(std::move(arg), sort_expr::direction::asc);
        });
    }
    // check_and_maybe_compile(arg);
    return std::make_unique<sort_use>(std::move(exprs));
  }
};

auto exec(std::string content, std::unique_ptr<diagnostic_handler> diag,
          const exec_config& cfg, caf::actor_system& sys) -> bool {
  auto content_view = std::string_view{content};
  auto tokens = tql2::tokenize(content);
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
          .emit(*diag);
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
  auto error_emitted = false;
  for (auto& token : tokens) {
    if (token.kind == tql2::token_kind::error) {
      auto begin = size_t{0};
      if (&token != tokens.data()) {
        begin = (&token - 1)->end;
      }
      diagnostic::error("could not parse token")
        .primary(location{begin, token.end})
        .emit(*diag);
      error_emitted = true;
    }
  }
  if (error_emitted) {
    return false;
  }
  class improve_me final : public diagnostic_handler {
  public:
    explicit improve_me(std::unique_ptr<diagnostic_handler> inner)
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
  auto im = improve_me{std::move(diag)};
  auto parsed = tql2::parse(tokens, content, im);
  if (im.error()) {
    return false;
  }
  auto reg = registry{};
  reg.add(std::make_unique<sort_definition>());
  // tql2::resolve_entities(parsed, reg, im);
  if (cfg.dump_ast) {
    with_thread_local_registry(reg, [&] {
      fmt::println("{:#?}", parsed);
    });
    return not im.error();
  }
  if (im.error()) {
    return false;
  }
  diagnostic::warning(
    "pipeline is syntactically valid, but execution is not yet implemented")
    .hint("use `--dump-ast` to show AST")
    .emit(im);
  return true;
}

} // namespace tenzir::tql2
