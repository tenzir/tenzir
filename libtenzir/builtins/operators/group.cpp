//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/compile_ctx.hpp>
#include <tenzir/exec.hpp>
#include <tenzir/finalize_ctx.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/substitute_ctx.hpp>

namespace tenzir::plugins::group {

namespace {

class group_exec final : public exec::operator_base {
public:
  group_exec() = default;

  group_exec(ast::expression over, ir::pipeline pipe, let_id id)
    : over_{std::move(over)}, pipe_{std::move(pipe)}, id_{id} {
  }

  auto name() const -> std::string override {
    return "group_exec";
  }

  friend auto inspect(auto& f, group_exec& x) -> bool {
    return f.object(x).fields(f.field("over", x.over_),
                              f.field("pipe", x.pipe_), f.field("id", x.id_));
  }

private:
  auto make_group(ast::constant::kind group, base_ctx ctx) const
    -> failure_or<exec::pipeline> {
    auto env = std::unordered_map<let_id, ast::constant::kind>{};
    env[id_] = std::move(group);
    auto copy = pipe_;
    TRY(copy.substitute(substitute_ctx{ctx, &env}, true));
    // TODO: Optimize it before finalize?
    return std::move(copy).finalize(finalize_ctx{ctx});
  }

  ast::expression over_;
  ir::pipeline pipe_;
  let_id id_;
};

class group_ir final : public ir::operator_base {
public:
  group_ir() = default;

  group_ir(ast::expression over, ir::pipeline pipe, let_id id)
    : over_{std::move(over)}, pipe_{std::move(pipe)}, id_{id} {
  }

  auto name() const -> std::string override {
    return "group_ir";
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    TRY(over_.substitute(ctx));
    // This operator instantiates the subpipeline on its own.
    (void)instantiate;
    TRY(pipe_.substitute(ctx, false));
    return {};
  }

  auto finalize(finalize_ctx ctx) && -> failure_or<exec::pipeline> override {
    (void)ctx;
    return std::make_unique<group_exec>(std::move(over_), std::move(pipe_),
                                        id_);
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

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<ir::operator_ptr> override {
    TENZIR_ASSERT(inv.args.size() == 2);
    auto over = std::move(inv.args[0]);
    TRY(over.bind(ctx));
    auto scope = ctx.open_scope();
    auto id = scope.let("group");
    auto pipe = as<ast::pipeline_expr>(inv.args[1]);
    TRY(auto pipe_ir, std::move(pipe.inner).compile(ctx));
    return std::make_unique<group_ir>(std::move(over), std::move(pipe_ir), id);
  }
};

using group_ir_plugin = inspection_plugin<ir::operator_base, group_ir>;
using group_exec_plugin = inspection_plugin<exec::operator_base, group_exec>;

} // namespace

} // namespace tenzir::plugins::group

TENZIR_REGISTER_PLUGIN(tenzir::plugins::group::group_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::group::group_ir_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::group::group_exec_plugin)
