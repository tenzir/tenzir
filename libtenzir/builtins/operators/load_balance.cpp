//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/let_id.hpp>
#include <tenzir/location.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/source.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/exec.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/type.hpp>
#include <tenzir/view.hpp>

#include <algorithm>
#include <deque>
#include <string>
#include <utility>
#include <vector>

namespace tenzir::plugins::load_balance {

namespace {

auto as_constant_kind(data const& value) -> ast::constant::kind {
  return match(value, []<class T>(T const& x) -> ast::constant::kind {
    if constexpr (std::same_as<T, pattern>) {
      TENZIR_UNREACHABLE();
    } else {
      return x;
    }
  });
}

struct LoadBalanceArgs {
  located<list> over;
  located<ir::pipeline> pipe;
  let_id binding_id;
};

class LoadBalance final : public Operator<table_slice, void> {
public:
  explicit LoadBalance(LoadBalanceArgs args) : args_{std::move(args)} {
  }

  auto start(JobId job, OpCtx& ctx) -> Task<void> override {
    if (started_) {
      co_return;
    }
    started_ = true;
    TENZIR_ASSERT(job.total <= args_.over.inner.size());
    workers_.assign(args_.over.inner.size(), Worker{.active = false});
    for (auto i = size_t{0}; i < args_.over.inner.size(); ++i) {
      if (i % job.total != job.index) {
        continue;
      }
      workers_[i].active = true;
      ++active_workers_;
      auto pipe = args_.pipe.inner;
      pipe.bind(args_.binding_id, as_constant_kind(args_.over.inner[i]));
      if (not co_await ctx.plan_and_spawn_sub<table_slice>(
            data{detail::narrow<int64_t>(i)}, std::move(pipe))) {
        done_ = true;
        co_return;
      }
    }
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    auto const rows = input.rows();
    while (active_workers_ > 0) {
      auto worker = select_worker();
      TENZIR_ASSERT(worker);
      auto sub = ctx.get_sub(detail::narrow<int64_t>(*worker));
      if (not sub) {
        mark_inactive(*worker);
        continue;
      }
      auto& pipe = as<SubHandle<table_slice>>(*sub);
      auto result = co_await pipe.push(std::move(input));
      if (result.is_ok()) {
        workers_[*worker].rows_assigned += rows;
        co_return;
      }
      input = std::move(result).unwrap_err();
      mark_inactive(*worker);
    }
    done_ = true;
  }

  auto finish_sub(SubKeyView key, OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(ctx);
    auto key_data = materialize(key);
    auto* index = try_as<int64_t>(key_data);
    TENZIR_ASSERT(index);
    mark_inactive(detail::narrow<size_t>(*index));
    co_return;
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    for (auto i = size_t{0}; i < workers_.size(); ++i) {
      if (not workers_[i].active) {
        continue;
      }
      if (auto sub = ctx.get_sub(detail::narrow<int64_t>(i))) {
        auto& pipe = as<SubHandle<table_slice>>(*sub);
        co_await pipe.close();
      }
      mark_inactive(i);
    }
    done_ = true;
    co_return FinalizeBehavior::done;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

private:
  struct Worker {
    uint64_t rows_assigned = 0;
    bool active = true;
  };

  auto select_worker() const -> Option<size_t> {
    auto result = Option<size_t>{None{}};
    for (auto i = size_t{0}; i < workers_.size(); ++i) {
      if (not workers_[i].active) {
        continue;
      }
      if (not result
          or workers_[i].rows_assigned < workers_[*result].rows_assigned) {
        result = i;
      }
    }
    return result;
  }

  auto mark_inactive(size_t index) -> void {
    TENZIR_ASSERT(index < workers_.size());
    if (not workers_[index].active) {
      return;
    }
    workers_[index].active = false;
    TENZIR_ASSERT(active_workers_ > 0);
    --active_workers_;
    if (active_workers_ == 0) {
      done_ = true;
    }
  }

  LoadBalanceArgs args_;
  std::vector<Worker> workers_;
  size_t active_workers_ = 0;
  bool started_ = false;
  bool done_ = false;
};

class LoadBalanceNova final : public Operator<nova::Events, void> {
public:
  explicit LoadBalanceNova(LoadBalanceArgs args) : args_{std::move(args)} {
  }

  auto start(JobId job, OpCtx& ctx) -> Task<void> override {
    if (started_) {
      co_return;
    }
    started_ = true;
    TENZIR_ASSERT(job.total <= args_.over.inner.size());
    if (done_) {
      co_return;
    }
    if (workers_.empty()) {
      workers_.assign(args_.over.inner.size(), Worker{.active = false});
      for (auto i = size_t{0}; i < args_.over.inner.size(); ++i) {
        if (i % job.total != job.index) {
          continue;
        }
        workers_[i].active = true;
        ++active_workers_;
      }
    }
    for (auto i = size_t{0}; i < args_.over.inner.size(); ++i) {
      if (not workers_[i].active) {
        continue;
      }
      auto pipe = args_.pipe.inner;
      pipe.bind(args_.binding_id, as_constant_kind(args_.over.inner[i]));
      if (not co_await ctx.plan_and_spawn_sub<nova::Events>(
            data{detail::narrow<int64_t>(i)}, std::move(pipe))) {
        done_ = true;
        co_return;
      }
    }
  }

  auto process(nova::Events input, OpCtx& ctx) -> Task<void> override {
    auto const rows = detail::narrow<uint64_t>(input.active_count());
    while (active_workers_ > 0) {
      auto worker = select_worker();
      TENZIR_ASSERT(worker);
      auto sub = ctx.get_sub(detail::narrow<int64_t>(*worker));
      if (not sub) {
        mark_inactive(*worker);
        continue;
      }
      auto& pipe = as<SubHandle<nova::Events>>(*sub);
      auto result = co_await pipe.push(std::move(input));
      if (result.is_ok()) {
        workers_[*worker].rows_assigned += rows;
        co_return;
      }
      input = std::move(result).unwrap_err();
      mark_inactive(*worker);
    }
    done_ = true;
  }

  auto finish_sub(SubKeyView key, OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(ctx);
    auto key_data = materialize(key);
    auto* index = try_as<int64_t>(key_data);
    TENZIR_ASSERT(index);
    mark_inactive(detail::narrow<size_t>(*index));
    co_return;
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    for (auto i = size_t{0}; i < workers_.size(); ++i) {
      if (not workers_[i].active) {
        continue;
      }
      if (auto sub = ctx.get_sub(detail::narrow<int64_t>(i))) {
        auto& pipe = as<SubHandle<nova::Events>>(*sub);
        co_await pipe.close();
      }
      mark_inactive(i);
    }
    done_ = true;
    co_return FinalizeBehavior::done;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

  auto snapshot(Serde& serde) -> void override {
    auto rows_assigned = std::vector<uint64_t>{};
    auto active = std::vector<uint8_t>{};
    if (not serde.is_loading()) {
      rows_assigned.reserve(workers_.size());
      active.reserve(workers_.size());
      for (auto const& worker : workers_) {
        rows_assigned.push_back(worker.rows_assigned);
        active.push_back(worker.active);
      }
    }
    serde("rows_assigned", rows_assigned);
    serde("active", active);
    serde("done", done_);
    if (serde.is_loading()) {
      TENZIR_ASSERT(rows_assigned.size() == active.size());
      TENZIR_ASSERT(rows_assigned.size() == args_.over.inner.size());
      workers_.clear();
      workers_.reserve(rows_assigned.size());
      active_workers_ = 0;
      for (auto i = size_t{0}; i < rows_assigned.size(); ++i) {
        workers_.push_back(Worker{rows_assigned[i], active[i] != 0});
        active_workers_ += active[i] != 0;
      }
      // Dynamic subpipelines do not survive a restore. Let start() recreate
      // only the workers that were active at the checkpoint.
      started_ = false;
    }
  }

private:
  struct Worker {
    uint64_t rows_assigned = 0;
    bool active = true;
  };

  auto select_worker() const -> Option<size_t> {
    auto result = Option<size_t>{None{}};
    for (auto i = size_t{0}; i < workers_.size(); ++i) {
      if (not workers_[i].active) {
        continue;
      }
      if (not result
          or workers_[i].rows_assigned < workers_[*result].rows_assigned) {
        result = i;
      }
    }
    return result;
  }

  auto mark_inactive(size_t index) -> void {
    TENZIR_ASSERT(index < workers_.size());
    if (not workers_[index].active) {
      return;
    }
    workers_[index].active = false;
    TENZIR_ASSERT(active_workers_ > 0);
    --active_workers_;
    if (active_workers_ == 0) {
      done_ = true;
    }
  }

  LoadBalanceArgs args_;
  std::vector<Worker> workers_;
  size_t active_workers_ = 0;
  bool started_ = false;
  bool done_ = false;
};

class LoadBalanceIr final : public ir::Operator {
public:
  LoadBalanceIr() = default;

  LoadBalanceIr(location self, ast::expression over_expr, let_id binding_id,
                located<ir::pipeline> pipe)
    : self_{self},
      over_expr_{std::move(over_expr)},
      binding_id_{binding_id},
      pipe_{std::move(pipe)} {
  }

  auto name() const -> std::string override {
    return "load_balance";
  }

  auto main_location() const -> location override {
    return self_;
  }

  auto copy() const -> Box<ir::Operator> override {
    return LoadBalanceIr{*this};
  }

  auto move() && -> Box<ir::Operator> override {
    return LoadBalanceIr{std::move(*this)};
  }

  auto max_parallelism() const -> size_t override {
    // Each target belongs to one replica, so never spawn more replicas than
    // targets. An idle sink replica would otherwise stop its upstream branch.
    return over_ ? over_->inner.size() : 1;
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    TRY(over_expr_.substitute(ctx));
    // The subpipeline is instantiated per worker at runtime, once its target
    // binding is known.
    TRY(pipe_.inner.substitute(ctx, false));
    if (instantiate) {
      TRY(auto constant, const_eval(over_expr_, base_ctx{ctx, ctx.reg()}));
      auto* entries = try_as<list>(constant.inner);
      if (not entries) {
        diagnostic::error("expected argument of type `list`, but got `{}`",
                          type_kind_of_data(constant.inner))
          .primary(over_expr_)
          .emit(ctx);
        return failure::promise();
      }
      if (entries->empty()) {
        diagnostic::error("expected list to not be empty")
          .primary(over_expr_)
          .emit(ctx);
        return failure::promise();
      }
      over_ = located{std::move(*entries), over_expr_.get_location()};
    }
    return {};
  }

  auto infer_type(element_type_tag input, base_ctx ctx) const
    -> failure_or<element_type_tag> override {
    if (input.is_not<table_slice>() and input.is_not<nova::Events>()) {
      diagnostic::error("operator expects events").primary(self_).emit(ctx);
      return failure::promise();
    }
    TRY(auto output, pipe_.inner.infer_type(input, ctx));
    if (output.is_not<void>()) {
      diagnostic::error("pipeline must currently end with a sink")
        .primary(pipe_.source)
        .emit(ctx);
      return failure::promise();
    }
    return tag_v<void>;
  }

  auto spawn(element_type_tag input, registry const&) const
    -> AnyOperator override {
    TENZIR_ASSERT(over_);
    auto args = LoadBalanceArgs{*over_, pipe_, binding_id_};
    if (input.is<nova::Events>()) {
      return LoadBalanceNova{std::move(args)}.with_name("load_balance");
    }
    TENZIR_ASSERT(input.is<table_slice>());
    return LoadBalance{std::move(args)}.with_name("load_balance");
  }

  auto optimize(ir::OptimizeRequest req,
                ir::OptimizeCtx const& octx) && -> ir::OptimizeResult override {
    auto sub = std::move(pipe_.inner)
                 .optimize(
                   ir::OptimizeRequest{
                     .filter = std::move(req.filter),
                     .order = req.order,
                     .limit = req.limit,
                     .projection = std::move(req.projection),
                   },
                   octx);
    pipe_.inner = std::move(sub.replacement);
    auto touched = ast::ExprRefs{};
    touched.let_ids.push_back(binding_id_);
    for (auto const& let : pipe_.inner.lets) {
      touched.let_ids.push_back(let.id);
    }
    auto [independent, dependent]
      = ir::split_filter_by_dependents(std::move(sub.filter), touched);
    for (auto const& expr : dependent) {
      ir::add_refs_to_projection(sub.projection, expr);
    }
    pipe_.inner.prepend(std::move(dependent), octx.reg);
    auto replacement = ir::pipeline{};
    replacement.operators.push_back(std::move(*this).move());
    return {
      .filter = std::move(independent),
      .order = EventOrder::unordered,
      .replacement = std::move(replacement),
      .limit = None{},
      .projection = std::move(sub.projection),
    };
  }

  friend auto inspect(auto& f, LoadBalanceIr& x) -> bool {
    return f.object(x).fields(f.field("self", x.self_),
                              f.field("over_expr", x.over_expr_),
                              f.field("over", x.over_),
                              f.field("binding_id", x.binding_id_),
                              f.field("pipe", x.pipe_));
  }

private:
  location self_;
  ast::expression over_expr_;
  Option<located<list>> over_;
  let_id binding_id_;
  located<ir::pipeline> pipe_;
};

class plugin final : public virtual operator_compiler_plugin {
public:
  auto name() const -> std::string override {
    return "load_balance";
  }

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<ir::CompileResult> override {
    // We use `operator_compiler_plugin` rather than `OperatorPlugin`/`Describer`
    // because named bindings (`load_balance $target=<list> { ... }`) choose the
    // subpipeline `let` binding name dynamically.
    constexpr auto usage
      = "load_balance $name=<list> { ... } or load_balance <list> { ... }";
    constexpr auto docs
      = "https://tenzir.com/docs/reference/operators/load_balance";
    auto loc = inv.op.get_location();
    auto var_name = Option<std::string>{};
    auto over_expr = Option<ast::expression>{};
    auto over_loc = Option<location>{};
    auto pipe_expr = Option<ast::pipeline_expr>{};
    auto saw_target_arg = false;
    auto failed = false;
    for (auto& arg : inv.args) {
      if (auto* pe = try_as<ast::pipeline_expr>(arg)) {
        if (pipe_expr) {
          diagnostic::error("duplicate pipeline argument")
            .primary(arg)
            .secondary(pipe_expr->get_location(), "previously provided here")
            .emit(ctx);
          failed = true;
          continue;
        }
        pipe_expr = std::move(*pe);
        continue;
      }
      saw_target_arg = true;
      if (over_expr) {
        diagnostic::error("expected a single list to load-balance over")
          .primary(arg)
          .secondary(*over_loc, "previously provided here")
          .docs(docs)
          .emit(ctx);
        failed = true;
        continue;
      }
      if (auto* assignment = try_as<ast::assignment>(arg)) {
        if (auto* var = try_as<ast::dollar_var>(assignment->left)) {
          var_name = std::string{var->name_without_dollar()};
          over_loc = arg.get_location();
          over_expr = std::move(assignment->right);
          continue;
        }
        if (auto* rf = try_as<ast::root_field>(assignment->left);
            rf and not rf->has_question_mark) {
          if (rf->id.name == "over") {
            over_loc = arg.get_location();
            over_expr = std::move(assignment->right);
            continue;
          }
          diagnostic::error("binding must start with `$`")
            .primary(assignment->left)
            .hint("use `${}=...` to bind `${}` in the subpipeline", rf->id.name,
                  rf->id.name)
            .docs(docs)
            .emit(ctx);
          failed = true;
          continue;
        }
        diagnostic::error("expected variable name")
          .primary(assignment->left)
          .hint("use a variable name such as `$target`")
          .docs(docs)
          .emit(ctx);
        failed = true;
        continue;
      }
      over_loc = arg.get_location();
      over_expr = std::move(arg);
    }
    if (not saw_target_arg) {
      diagnostic::error("expected a list or binding `$name=<list>`")
        .primary(loc)
        .usage(usage)
        .docs(docs)
        .emit(ctx);
      failed = true;
    }
    if (not pipe_expr) {
      diagnostic::error("required subpipeline was not provided")
        .primary(loc)
        .usage(usage)
        .docs(docs)
        .emit(ctx);
      failed = true;
    }
    if (failed) {
      return failure::promise();
    }
    TENZIR_ASSERT(over_expr);
    // Bind the target list expression against the enclosing scope before
    // opening the subpipeline scope, so `let $x = [...]; load_balance $x=$x`
    // reads the outer list on the RHS and binds `$x` inside the subpipeline.
    TRY(over_expr->bind(ctx));
    auto scope = ctx.open_scope();
    auto binding_id
      = scope.let(var_name ? std::move(*var_name) : std::string{"over"});
    auto pipe_loc = pipe_expr->get_location();
    TRY(auto pipe_ir, std::move(pipe_expr->inner).compile(ctx));
    return LoadBalanceIr{
      loc,
      std::move(*over_expr),
      binding_id,
      located{std::move(pipe_ir), pipe_loc},
    };
  }
};

} // namespace

} // namespace tenzir::plugins::load_balance

TENZIR_REGISTER_PLUGIN(tenzir::plugins::load_balance::plugin)
TENZIR_REGISTER_PLUGIN(
  tenzir::inspection_plugin<tenzir::ir::Operator,
                            tenzir::plugins::load_balance::LoadBalanceIr>)
