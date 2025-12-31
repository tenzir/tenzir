//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tql2/eval.hpp>

namespace tenzir::plugins::head {

namespace {

class Head final : public Operator<table_slice, table_slice> {
public:
  explicit Head(int64_t count) : remaining_{count} {
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    // TODO: Do we want to guarantee this?
    TENZIR_ASSERT(remaining_ > 0);
    auto result = tenzir::head(input, remaining_);
    TENZIR_ASSERT(result.rows() <= remaining_);
    remaining_ -= result.rows();
    co_await push(std::move(result));
  }

  auto state() -> OperatorState override {
    if (remaining_ == 0) {
      // TODO: We also want to declare that we'll produce no more output and
      // that we are ready to shutdown.
      return OperatorState::done;
    }
    return OperatorState::unspecified;
  }

  auto snapshot(Serde& serde) -> void override {
    serde("remaining", remaining_);
  }

private:
  int64_t remaining_;
};

class head_ir final : public ir::Operator {
public:
  head_ir() = default;

  explicit head_ir(location self, ast::expression count)
    : self_{self}, count_{std::move(count)} {
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

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<std::optional<element_type_tag>> override {
    // TODO: Refactor.
    if (not input.is<table_slice>()) {
      diagnostic::error("expected events, got {}", input)
        .primary(main_location())
        .emit(dh);
      return failure::promise();
    }
    return element_type_tag{tag_v<table_slice>};
  }

  auto spawn(element_type_tag input) && -> AnyOperator override {
    TENZIR_ASSERT(input.is<table_slice>());
    return Head{as<int64_t>(count_)};
  }

  auto main_location() const -> location override {
    return self_;
  }

  friend auto inspect(auto& f, head_ir& x) -> bool {
    return f.object(x).fields(f.field("self", x.self_),
                              f.field("count", x.count_));
  }

private:
  location self_;
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
    -> failure_or<Box<ir::Operator>> override {
    // TODO: Actual parsing.
    if (inv.args.size() > 1) {
      diagnostic::error("expected exactly one argument")
        .primary(inv.op)
        .emit(ctx);
      return failure::promise();
    }
    auto expr = inv.args.empty() ? ast::constant{10, location::unknown}
                                 : std::move(inv.args[0]);
    return head_ir{inv.op.get_location(), std::move(expr)};
  }
};

} // namespace

} // namespace tenzir::plugins::head

TENZIR_REGISTER_PLUGIN(tenzir::plugins::head::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::inspection_plugin<
                       tenzir::ir::Operator, tenzir::plugins::head::head_ir>)
