//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async.hpp>
#include <tenzir/async/queue_scope.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/substitute_ctx.hpp>

#include <folly/Expected.h>

namespace tenzir::plugins::group {

namespace {

template <class Output>
class Group final : public Operator<table_slice, Output> {
public:
  Group(ast::expression over, ir::pipeline pipe, let_id let_id)
    : over_{std::move(over)}, pipe_{std::move(pipe)}, let_id_{let_id} {
  }

  auto process(table_slice input, Push<Output>& push,
               OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(push);
    // TODO
    auto key_value = ast::constant::kind{"hi"};
    auto key_data = match(key_value, [&](auto& x) {
      return data{x};
    });
    auto sub = ctx.get_sub(make_view(key_data));
    if (not sub) {
      auto env = substitute_ctx::env_t{};
      env[let_id_] = std::move(key_value);
      auto sub_ctx = substitute_ctx{ctx, &env};
      auto copy = pipe_;
      if (not copy.substitute(sub_ctx, true)) {
        co_return;
      }
      sub = co_await ctx.spawn_sub(std::move(key_data), std::move(copy),
                                   tag_v<table_slice>);
    }
    TENZIR_ASSERT(sub);
    auto& cast = as<OpenPipeline<table_slice>>(*sub);
    auto result = co_await cast.push(std::move(input));
    auto closed = result.is_err();
    if (closed) {
      // TODO: Ignore?
    }
  }

  auto snapshot(Serde& serde) -> void override {
    TENZIR_UNUSED(serde);
    // serde("pipes", pipes_);
  }

private:
  ast::expression over_;
  ir::pipeline pipe_;
  let_id let_id_;

  // tsl::robin_map<data, OpenPipeline> pipes_;

  // TODO: Not serializable, plus where is restore logic?
  // The operator implementation should be in control when to consume data.
  // However… We do not to be able to consume checkpoint infos etc! If we don't
  // consume data, then the checkpoint is delayed. Maybe that is to be expected?
  // TODO: How do we handle errors in subpipelines?
  // TODO: Activation of scope?
  // mutable Box<QueueScope<std::optional<table_slice>>> pipe_output_;
};

class group_ir final : public ir::Operator {
public:
  group_ir() = default;

  group_ir(ast::expression over, ir::pipeline pipe, let_id id)
    : over_{std::move(over)}, pipe_{std::move(pipe)}, id_{id} {
  }

  auto name() const -> std::string override {
    return "group_ir";
  }

  auto substitute(substitute_ctx ctx,
                  bool instantiate) -> failure_or<void> override {
    TRY(over_.substitute(ctx));
    // This operator instantiates the subpipeline on its own.
    (void)instantiate;
    TRY(pipe_.substitute(ctx, false));
    return {};
  }

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<std::optional<element_type_tag>> override {
    return pipe_.infer_type(input, dh);
  }

  auto spawn(element_type_tag input) && -> AnyOperator override {
    TENZIR_ASSERT(input.is<table_slice>());
    return Group<table_slice>{std::move(over_), std::move(pipe_), id_};
  }

  friend auto inspect(auto& f, group_ir& x) -> bool {
    return f.object(x).fields(f.field("over", x.over_),
                              f.field("pipe", x.pipe_), f.field("id", x.id_));
  }

private:
  ast::expression over_;
  ir::pipeline pipe_;
  let_id id_;
};

class group_plugin final : public operator_compiler_plugin {
public:
  auto name() const -> std::string override {
    return "group";
  }

  auto
  compile(ast::invocation inv,
          compile_ctx ctx) const -> failure_or<Box<ir::Operator>> override {
    TENZIR_ASSERT(inv.args.size() == 2);
    auto over = std::move(inv.args[0]);
    TRY(over.bind(ctx));
    auto scope = ctx.open_scope();
    auto id = scope.let("group");
    auto pipe = as<ast::pipeline_expr>(inv.args[1]);
    TRY(auto pipe_ir, std::move(pipe.inner).compile(ctx));
    return group_ir{std::move(over), std::move(pipe_ir), id};
  }
};

using group_ir_plugin = inspection_plugin<ir::Operator, group_ir>;

} // namespace

} // namespace tenzir::plugins::group

TENZIR_REGISTER_PLUGIN(tenzir::plugins::group::group_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::group::group_ir_plugin)
