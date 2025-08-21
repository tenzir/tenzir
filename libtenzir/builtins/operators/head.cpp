//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/exec/operator.hpp>
#include <tenzir/exec/trampoline.hpp>
#include <tenzir/finalize_ctx.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plan/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tql2/eval.hpp>

#include <caf/actor_from_state.hpp>
#include <caf/actor_registry.hpp>

namespace tenzir::plugins::head {

namespace {

class head_exec : public exec::operator_base<uint64_t> {
public:
  explicit head_exec(initializer init) : operator_base{std::move(init)} {
  }

  void next(const table_slice& slice) override {
    auto& remaining = state();
    auto take = std::min(remaining, slice.rows());
    push(subslice(slice, 0, take));
    remaining -= take;
    ready();
  }

  auto should_stop() -> bool override {
    return get_input_ended() or state() == 0;
  }
};

class head_plan final : public plan::operator_base {
public:
  explicit head_plan(int64_t count) : count_{count} {
  }

  auto name() const -> std::string override {
    return "head_plan";
  }

  auto spawn(plan::operator_spawn_args args) const
    -> exec::operator_actor override {
    return exec::spawn_operator<head_exec>(std::move(args), count_);
  }

  friend auto inspect(auto& f, head_plan& x) -> bool {
    return f.apply(x.count_);
  }

private:
  int64_t count_;
};

class head_ir final : public ir::operator_base {
public:
  head_ir() = default;

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

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<std::optional<element_type_tag>> override {
    // TODO!
    (void)dh;
    TENZIR_ASSERT(input.is<table_slice>());
    return element_type_tag{tag_v<table_slice>};
  }

  auto finalize(finalize_ctx ctx) && -> failure_or<plan::pipeline> override {
    (void)ctx;
    return std::make_unique<head_plan>(as<int64_t>(count_));
  }

  friend auto inspect(auto& f, head_ir& x) -> bool {
    return f.apply(x.count_);
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
    if (inv.args.size() > 1) {
      diagnostic::error("expected exactly one argument")
        .primary(inv.op)
        .emit(ctx);
      return failure::promise();
    }
    auto expr = inv.args.empty() ? ast::constant{10, location::unknown}
                                 : std::move(inv.args[0]);
    return std::make_unique<head_ir>(std::move(expr));
  }
};

} // namespace

} // namespace tenzir::plugins::head

TENZIR_REGISTER_PLUGIN(tenzir::plugins::head::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::inspection_plugin<tenzir::ir::operator_base,
                                                 tenzir::plugins::head::head_ir>)
