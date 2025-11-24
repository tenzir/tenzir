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
#include <tenzir/exec/pipeline.hpp>
#include <tenzir/finalize_ctx.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/plan/operator.hpp>
#include <tenzir/substitute_ctx.hpp>

#include <caf/actor_from_state.hpp>
#include <folly/Expected.h>

namespace tenzir::plugins::group {

namespace {

#if 0
class group {
public:
  group(exec::operator_actor::pointer self, ast::expression over,
        ir::pipeline pipe, let_id id, base_ctx ctx)
    : self_{self},
      over_{std::move(over)},
      pipe_{std::move(pipe)},
      id_{id},
      ctx_{ctx} {
  }

  auto make_behavior() -> exec::operator_actor::behavior_type {
    TENZIR_TODO();
    // return {
    //   [](exec::handshake hs) -> caf::result<exec::handshake_response> {
    //     (void)hs;
    //     TENZIR_TODO();
    //   },
    //   [](exec::checkpoint) -> caf::result<void> {
    //     TENZIR_TODO();
    //   },
    //   [](atom::stop) -> caf::result<void> {
    //     TENZIR_TODO();
    //   },
    // };
  }

  // TODO: Do this properly.
  void process(table_slice slice) {
    for (auto i = size_t{0}; i < slice.rows(); ++i) {
      auto value = std::string{"hello"};
      auto it = groups_.find(value);
      if (it == groups_.end()) {
        // TODO: What if fail?
        auto group_bp = make_group(value).unwrap();
        // TODO: The inner executor does not have checkpoint settings!
        auto new_group = group_t{
          exec::make_pipeline(std::move(group_bp), exec::pipeline_settings{},
                              std::nullopt, ctx_),
        };
        auto [new_it, inserted]
          = groups_.try_emplace(std::move(value), std::move(new_group));
        TENZIR_ASSERT(inserted);
        it = new_it;
      }
      // Assume we send it into the CAF flow (of course not like that).
      // send(group, slice.row(i));
    }
  }

  friend auto inspect(auto& f, group& x) -> bool {
    // TODO: We cannot inspect `self_`, `ctx_` and actor handles.
    return f.object(x).fields();
  }

private:
  struct group_t {
    exec::subpipeline_actor exec;
    // TODO: Somehow we have to inject data into the flow?

    friend auto inspect(auto& f, group_t& x) -> bool {
      TENZIR_UNUSED(f, x);
      TENZIR_TODO();
    }
  };

  auto make_group(ast::constant::kind group) const
    -> failure_or<plan::pipeline> {
    auto env = std::unordered_map<let_id, ast::constant::kind>{};
    env[id_] = std::move(group);
    auto copy = pipe_;
    TRY(copy.substitute(substitute_ctx{ctx_, &env}, true));
    // TODO: Optimize it before finalize?
    return std::move(copy).finalize(finalize_ctx{ctx_});
  }

  exec::operator_actor::pointer self_;
  ast::expression over_;
  ir::pipeline pipe_;
  let_id id_;
  // TODO: Accept arbitrary constants as keys.
  std::unordered_map<std::string, group_t> groups_;
  base_ctx ctx_;
};
#endif

template <class Value, class Error>
class [[nodiscard]] Result {
public:
};

class OpenPipeline {
public:
  auto push(table_slice input) -> Task<Result<void, table_slice>> {
    TENZIR_TODO();
  }

  auto close_input() -> void {
    TENZIR_TODO();
  }

  auto pull() -> Task<std::optional<table_slice>> {
    TENZIR_TODO();
  }
};

template <class Output>
class Group final : public Operator<table_slice, Output> {
public:
  Group(ast::expression over, ir::pipeline pipe)
    : over_{std::move(over)}, pipe_{std::move(pipe)} {
  }

  auto await_task() const -> Task<std::any> override {
    co_return co_await pipe_output_->next();
  }

  auto process(table_slice input, Push<Output>& push, AsyncCtx& ctx)
    -> Task<void> override {
    // TODO
    auto key = data{"hi"};
    auto it = pipes_.find(key);
    // TODO
    TENZIR_ASSERT(it != pipes_.end());
    auto& pipe = it.value();
    // TODO: Earlier, when spawning the pipeline
    pipe_output_->spawn(pipe.pull());
    (void)co_await pipe.pull();
    co_return;
  }

private:
  ast::expression over_;
  ir::pipeline pipe_;

  tsl::robin_map<data, OpenPipeline> pipes_;

  // TODO: Not serializable, plus where is restore logic?
  // The operator implementation should be in control when to consume data.
  // However… We do not to be able to consume checkpoint infos etc! If we don't
  // consume data, then the checkpoint is delayed. Maybe that is to be expected?
  // TODO: How do we handle errors in subpipelines?
  // TODO: Activation of scope?
  mutable Box<QueueScope<std::optional<table_slice>>> pipe_output_;
};

class group_bp final : public plan::operator_base {
public:
  group_bp() = default;

  group_bp(ast::expression over, ir::pipeline pipe, let_id id)
    : over_{std::move(over)}, pipe_{std::move(pipe)}, id_{id} {
  }

  auto name() const -> std::string override {
    return "group_bp";
  }

  auto spawn(plan::operator_spawn_args args) const
    -> exec::operator_actor override {
    TENZIR_TODO();
    // return args.sys.spawn(caf::actor_from_state<group>, over_, pipe_, id_,
    //                       args.ctx);
  }

  auto spawn(std::optional<chunk_ptr> restore) && -> AnyOperator {
    Box<Operator<table_slice, table_slice>> test
      = Group<table_slice>{std::move(over_), std::move(pipe_)};
    return test;
    // return Group<table_slice>{std::move(over_), std::move(pipe_)};
  }

  friend auto inspect(auto& f, group_bp& x) -> bool {
    return f.object(x).fields(f.field("over", x.over_),
                              f.field("pipe", x.pipe_), f.field("id", x.id_));
  }

private:
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

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<std::optional<element_type_tag>> override {
    return pipe_.infer_type(input, dh);
  }

  auto finalize(finalize_ctx ctx) && -> failure_or<plan::pipeline> override {
    (void)ctx;
    return std::make_unique<group_bp>(std::move(over_), std::move(pipe_), id_);
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
using group_exec_plugin = inspection_plugin<plan::operator_base, group_bp>;

} // namespace

} // namespace tenzir::plugins::group

TENZIR_REGISTER_PLUGIN(tenzir::plugins::group::group_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::group::group_ir_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::group::group_exec_plugin)
