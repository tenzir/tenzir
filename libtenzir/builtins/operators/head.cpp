//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/finalize_ctx.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tql2/eval.hpp>

namespace tenzir::plugins::head {

namespace {

class head_bp final : public bp::operator_base {
public:
  explicit head_bp(int64_t count) : count_{count} {
  }

  auto name() const -> std::string override {
    return "head_bp";
  }

private:
  int64_t count_;
};

class head_ir final : public ir::operator_base {
public:
  explicit head_ir(ast::expression count) : count_{std::move(count)} {
  }

  auto name() const -> std::string override {
    return "head_ir";
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    if (auto expr = try_as<ast::expression>(count_)) {
      TRY(expr->substitute(ctx));
      if (instantiate or expr->is_deterministic(ctx)) {
        TRY(auto value, const_eval(*expr, ctx));
        auto count = try_as<int64_t>(value);
        if (not count or *count < 0) {
          diagnostic::error("TODO").primary(*expr).emit(ctx);
          return failure::promise();
        }
        count_ = *count;
      }
    }
    return {};
  }

  auto finalize(finalize_ctx ctx) && -> failure_or<bp::pipeline> override {
    return std::make_unique<head_bp>(as<int64_t>(count_));
  }

private:
  variant<ast::expression, int64_t> count_;
};

class plugin final : public virtual operator_parser_plugin,
                     public virtual operator_compiler_plugin {
public:
  auto name() const -> std::string override {
    return "head";
  };

  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"head", "https://docs.tenzir.com/"
                                          "operators/head"};
    auto count = std::optional<uint64_t>{};
    parser.add(count, "<limit>");
    parser.parse(p);
    auto result = pipeline::internal_parse_as_operator(
      fmt::format("slice :{}", count.value_or(10)));
    if (not result) {
      diagnostic::error("failed to transform `head` into `slice` operator: {}",
                        result.error())
        .throw_();
    }
    return std::move(*result);
  }

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<ir::operator_ptr> override {
    // TODO: Actual parsing.
    TENZIR_ASSERT(inv.args.size() == 1);
    return std::make_unique<head_ir>(std::move(inv.args[0]));
  }
};

} // namespace

} // namespace tenzir::plugins::head

TENZIR_REGISTER_PLUGIN(tenzir::plugins::head::plugin)
