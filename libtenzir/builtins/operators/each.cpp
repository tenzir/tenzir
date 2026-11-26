//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/let_id.hpp>
#include <tenzir/location.hpp>
#include <tenzir/nova/bitmap_iteration.hpp>
#include <tenzir/nova/eval.hpp>
#include <tenzir/nova/materialize.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/type.hpp>
#include <tenzir/view3.hpp>

#include <algorithm>
#include <deque>
#include <limits>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace tenzir::plugins::each {

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

struct EachBinding {
  ast::expression expr;
  let_id let;

  friend auto inspect(auto& f, EachBinding& x) -> bool {
    return f.object(x).fields(f.field("expr", x.expr), f.field("let", x.let));
  }
};

struct EachArgs {
  std::vector<EachBinding> bindings;
  located<ir::pipeline> pipe;
  uint64_t parallel = 10;
};

using RowValues = std::vector<ast::constant::kind>;

auto bind_row_values(ir::pipeline& pipe,
                     std::vector<EachBinding> const& bindings, RowValues values)
  -> void {
  TENZIR_ASSERT(values.size() == bindings.size());
  for (auto i = size_t{0}; i < bindings.size(); ++i) {
    pipe.bind(bindings[i].let, std::move(values[i]));
  }
}

auto extract_row(std::vector<multi_series> const& columns, int64_t row)
  -> RowValues {
  auto values = RowValues{};
  values.reserve(columns.size());
  for (auto const& column : columns) {
    values.push_back(as_constant_kind(materialize(column.view3_at(row))));
  }
  return values;
}

struct EachImpl {
  struct PendingSlice {
    table_slice input;
    std::vector<multi_series> columns;
    int64_t next_row = 0;
    int64_t total_rows = 0;
  };

  explicit EachImpl(EachArgs args) : args_{std::move(args)} {
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> {
    if (failed_ or input.rows() == 0) {
      co_return;
    }
    TENZIR_ASSERT(not todo_);
    TENZIR_ASSERT(restored_.empty());
    TENZIR_ASSERT(running_subs_ < args_.parallel);
    auto columns = std::vector<multi_series>{};
    columns.reserve(args_.bindings.size());
    for (auto const& binding : args_.bindings) {
      columns.push_back(eval(binding.expr, input, ctx));
    }
    auto const rows = detail::narrow<int64_t>(input.rows());
    auto const take = detail::narrow<int64_t>(
      std::min<uint64_t>(args_.parallel - running_subs_, input.rows()));
    auto immediate = std::vector<RowValues>{};
    immediate.reserve(detail::narrow<size_t>(take));
    for (auto row = int64_t{0}; row < take; ++row) {
      immediate.push_back(extract_row(columns, row));
    }
    if (take < rows) {
      todo_ = PendingSlice{
        .input = std::move(input),
        .columns = std::move(columns),
        .next_row = take,
        .total_rows = rows,
      };
    }
    for (auto& values : immediate) {
      co_await spawn_for(std::move(values), ctx);
      if (failed_) {
        todo_ = None{};
        restored_.clear();
        co_return;
      }
    }
  }

  auto spawn_for(RowValues values, OpCtx& ctx) -> Task<void> {
    auto copy = args_.pipe.inner;
    bind_row_values(copy, args_.bindings, std::move(values));
    if (not co_await ctx.plan_and_spawn_sub<void>(data{next_key_},
                                                  std::move(copy))) {
      // Planning emitted a diagnostic; the pipeline will be torn down.
      failed_ = true;
      co_return;
    }
    next_key_ += 1;
    running_subs_ += 1;
  }

  auto finish_sub(SubKeyView key, OpCtx& ctx) -> Task<void> {
    TENZIR_UNUSED(key, ctx);
    TENZIR_ASSERT(running_subs_ > 0);
    running_subs_ -= 1;
    if (failed_) {
      co_return;
    }
    if (todo_) {
      auto values = extract_row(todo_->columns, todo_->next_row);
      todo_->next_row += 1;
      if (todo_->next_row == todo_->total_rows) {
        todo_ = None{};
      }
      co_await spawn_for(std::move(values), ctx);
    } else if (not restored_.empty()) {
      auto values = std::move(restored_.front());
      restored_.pop_front();
      co_await spawn_for(std::move(values), ctx);
    }
  }

  auto state() -> OperatorState {
    if (failed_) {
      return OperatorState::done;
    }
    return running_subs_ < args_.parallel and not todo_ and restored_.empty()
             ? OperatorState::normal
             : OperatorState::blocked;
  }

  auto snapshot(Serde& serde) -> void {
    serde("next_key", next_key_);
    serde("running_subs", running_subs_);
    if (serde.is_loading()) {
      serde("pending", restored_);
      todo_ = None{};
      return;
    }
    auto pending = std::deque<RowValues>{};
    if (todo_) {
      for (auto row = todo_->next_row; row < todo_->total_rows; ++row) {
        pending.push_back(extract_row(todo_->columns, row));
      }
    }
    pending.insert(pending.end(), restored_.begin(), restored_.end());
    serde("pending", pending);
  }

  EachArgs args_;
  uint64_t next_key_ = 0;
  size_t running_subs_ = 0;
  Option<PendingSlice> todo_ = None{};
  std::deque<RowValues> restored_;
  bool failed_ = false;
};

/// Spawns a source subpipeline per event, forwarding their event output.
class Each final : public Operator<table_slice, table_slice>, private EachImpl {
public:
  using EachImpl::EachImpl;

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push);
    return EachImpl::process(std::move(input), ctx);
  }

  auto finish_sub(SubKeyView key, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push);
    return EachImpl::finish_sub(key, ctx);
  }

  auto state() -> OperatorState override {
    return EachImpl::state();
  }

  auto snapshot(Serde& serde) -> void override {
    EachImpl::snapshot(serde);
  }
};

/// Spawns a source subpipeline per event; subpipelines sink their own output.
class EachSink final : public Operator<table_slice, void>, private EachImpl {
public:
  using EachImpl::EachImpl;

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    return EachImpl::process(std::move(input), ctx);
  }

  auto finish_sub(SubKeyView key, OpCtx& ctx) -> Task<void> override {
    return EachImpl::finish_sub(key, ctx);
  }

  auto state() -> OperatorState override {
    return EachImpl::state();
  }

  auto snapshot(Serde& serde) -> void override {
    EachImpl::snapshot(serde);
  }
};

auto materialize_constant(nova::Events const& events,
                          nova::storage::Index index) -> ast::constant::kind {
  return as_constant_kind(nova::materialize_legacy(
    nova::RowView<nova::Data>{events.data.get(index)}));
}

auto extract_nova_row(
  nova::Events const& input,
  std::vector<Option<nova::Array<nova::Data>>> const& columns,
  nova::storage::Index index) -> RowValues {
  auto values = RowValues{};
  values.reserve(columns.size());
  for (auto const& column : columns) {
    if (column) {
      values.push_back(
        as_constant_kind(nova::materialize_legacy(column->get(index))));
    } else {
      values.push_back(materialize_constant(input, index));
    }
  }
  return values;
}

struct EachNovaImpl {
  struct PendingBatch {
    nova::Events input;
    std::vector<Option<nova::Array<nova::Data>>> columns;
  };

  explicit EachNovaImpl(EachArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> {
    evaluators_.reserve(args_.bindings.size());
    for (auto& binding : args_.bindings) {
      if (is<ast::this_>(binding.expr)) {
        evaluators_.push_back(None{});
        continue;
      }
      auto evaluator
        = co_await nova::Evaluator::make(std::move(binding.expr), ctx);
      if (not evaluator) {
        failed_ = true;
        co_return;
      }
      evaluators_.push_back(std::move(*evaluator));
    }
    co_return;
  }

  auto process(nova::Events input, OpCtx& ctx) -> Task<void> {
    if (failed_ or not input.mask.any()) {
      co_return;
    }
    TENZIR_ASSERT(not todo_);
    TENZIR_ASSERT(restored_.empty());
    TENZIR_ASSERT(running_subs_ < args_.parallel);
    TENZIR_ASSERT(evaluators_.size() == args_.bindings.size());
    auto columns = std::vector<Option<nova::Array<nova::Data>>>{};
    columns.reserve(evaluators_.size());
    for (auto& evaluator : evaluators_) {
      if (evaluator) {
        columns.push_back(evaluator->eval(input, nova::EvalCtx{ctx.dh()}));
      } else {
        columns.push_back(None{});
      }
    }
    auto const take = args_.parallel - running_subs_;
    auto remaining = nova::storage::BitMap::Mutable{input.mask};
    auto immediate = std::vector<RowValues>{};
    immediate.reserve(std::min<uint64_t>(take, input.active_count()));
    for (auto index : nova::storage::true_bits(input.mask)) {
      if (immediate.size() == take) {
        break;
      }
      remaining.set(index, false);
      immediate.push_back(extract_nova_row(input, columns, index));
    }
    input.mask = std::move(remaining).finish();
    if (input.mask.any()) {
      todo_ = PendingBatch{
        .input = std::move(input),
        .columns = std::move(columns),
      };
    }
    for (auto& values : immediate) {
      co_await spawn_for(std::move(values), ctx);
      if (failed_) {
        todo_ = None{};
        restored_.clear();
        co_return;
      }
    }
  }

  auto spawn_for(RowValues values, OpCtx& ctx) -> Task<void> {
    auto copy = args_.pipe.inner;
    bind_row_values(copy, args_.bindings, std::move(values));
    if (not co_await ctx.plan_and_spawn_sub<void>(data{next_key_},
                                                  std::move(copy))) {
      // Planning emitted a diagnostic; the pipeline will be torn down.
      failed_ = true;
      co_return;
    }
    next_key_ += 1;
    running_subs_ += 1;
  }

  auto finish_sub(SubKeyView key, OpCtx& ctx) -> Task<void> {
    TENZIR_UNUSED(key, ctx);
    TENZIR_ASSERT(running_subs_ > 0);
    running_subs_ -= 1;
    if (failed_) {
      co_return;
    }
    if (todo_) {
      auto index = *nova::storage::true_bits(todo_->input.mask).begin();
      auto values = extract_nova_row(todo_->input, todo_->columns, index);
      auto remaining
        = nova::storage::BitMap::Mutable{std::move(todo_->input.mask)};
      remaining.set(index, false);
      todo_->input.mask = std::move(remaining).finish();
      if (not todo_->input.mask.any()) {
        todo_ = None{};
      }
      co_await spawn_for(std::move(values), ctx);
    } else if (not restored_.empty()) {
      auto values = std::move(restored_.front());
      restored_.pop_front();
      co_await spawn_for(std::move(values), ctx);
    }
  }

  auto state() -> OperatorState {
    if (failed_) {
      return OperatorState::done;
    }
    return running_subs_ < args_.parallel and not todo_ and restored_.empty()
             ? OperatorState::normal
             : OperatorState::blocked;
  }

  auto snapshot(Serde& serde) -> void {
    serde("next_key", next_key_);
    serde("running_subs", running_subs_);
    if (serde.is_loading()) {
      serde("pending", restored_);
      todo_ = None{};
      return;
    }
    auto pending = std::deque<RowValues>{};
    if (todo_) {
      for (auto index : nova::storage::true_bits(todo_->input.mask)) {
        pending.push_back(
          extract_nova_row(todo_->input, todo_->columns, index));
      }
    }
    pending.insert(pending.end(), restored_.begin(), restored_.end());
    serde("pending", pending);
  }

  EachArgs args_;
  std::vector<Option<nova::Evaluator>> evaluators_;
  uint64_t next_key_ = 0;
  size_t running_subs_ = 0;
  Option<PendingBatch> todo_ = None{};
  std::deque<RowValues> restored_;
  bool failed_ = false;
};

class EachNova final : public Operator<nova::Events, nova::Events>,
                       private EachNovaImpl {
public:
  using EachNovaImpl::EachNovaImpl;

  auto start(OpCtx& ctx) -> Task<void> override {
    return EachNovaImpl::start(ctx);
  }

  auto process(nova::Events input, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push);
    return EachNovaImpl::process(std::move(input), ctx);
  }

  auto finish_sub(SubKeyView key, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push);
    return EachNovaImpl::finish_sub(key, ctx);
  }

  auto state() -> OperatorState override {
    return EachNovaImpl::state();
  }

  auto snapshot(Serde& serde) -> void override {
    EachNovaImpl::snapshot(serde);
  }
};

class EachSinkNova final : public Operator<nova::Events, void>,
                           private EachNovaImpl {
public:
  using EachNovaImpl::EachNovaImpl;

  auto start(OpCtx& ctx) -> Task<void> override {
    return EachNovaImpl::start(ctx);
  }

  auto process(nova::Events input, OpCtx& ctx) -> Task<void> override {
    return EachNovaImpl::process(std::move(input), ctx);
  }

  auto finish_sub(SubKeyView key, OpCtx& ctx) -> Task<void> override {
    return EachNovaImpl::finish_sub(key, ctx);
  }

  auto state() -> OperatorState override {
    return EachNovaImpl::state();
  }

  auto snapshot(Serde& serde) -> void override {
    EachNovaImpl::snapshot(serde);
  }
};

class EachIr final : public ir::Operator {
public:
  EachIr() = default;

  EachIr(location self, std::vector<EachBinding> bindings,
         Option<ast::expression> parallel_expr, located<ir::pipeline> pipe)
    : self_{self},
      bindings_{std::move(bindings)},
      parallel_expr_{std::move(parallel_expr)},
      pipe_{std::move(pipe)} {
  }

  auto name() const -> std::string override {
    return "each";
  }

  auto main_location() const -> location override {
    return self_;
  }

  auto copy() const -> Box<ir::Operator> override {
    return EachIr{*this};
  }

  auto move() && -> Box<ir::Operator> override {
    return EachIr{std::move(*this)};
  }

  auto concurrent_output() const -> bool override {
    return true;
  }

  auto max_parallelism() const -> size_t override {
    // Every instance owns its child pipelines and completion state. The
    // `parallel` argument therefore remains a per-instance bound, making the
    // pipeline-wide child-pipeline budget `parallel` times the planner degree.
    return std::numeric_limits<size_t>::max();
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    for (auto& binding : bindings_) {
      TRY(binding.expr.substitute(ctx));
    }
    if (parallel_expr_) {
      TRY(parallel_expr_->substitute(ctx));
    }
    // The subpipeline is instantiated per event at runtime, once its event
    // bindings are known.
    TRY(pipe_.inner.substitute(ctx, false));
    if (instantiate and parallel_expr_) {
      TRY(auto constant, const_eval(*parallel_expr_, base_ctx{ctx, ctx.reg()}));
      auto value = std::move(constant.inner);
      if (auto* integer = try_as<int64_t>(value)) {
        if (*integer < 0) {
          diagnostic::error("expected positive integer, got `{}`", *integer)
            .primary(*parallel_expr_)
            .emit(ctx);
          return failure::promise();
        }
        value = detail::narrow<uint64_t>(*integer);
      }
      auto* parsed = try_as<uint64_t>(value);
      if (not parsed) {
        diagnostic::error("expected argument of type `uint64`, but got `{}`",
                          type_kind_of_data(value))
          .primary(*parallel_expr_)
          .emit(ctx);
        return failure::promise();
      }
      if (*parsed < 1) {
        diagnostic::error("`parallel` must be at least 1")
          .primary(*parallel_expr_)
          .emit(ctx);
        return failure::promise();
      }
      parallel_ = *parsed;
    }
    return {};
  }

  auto infer_type(element_type_tag input, base_ctx ctx) const
    -> failure_or<element_type_tag> override {
    if (input.is_not<table_slice>() and input.is_not<nova::Events>()) {
      diagnostic::error("operator expects events").primary(self_).emit(ctx);
      return failure::promise();
    }
    auto null_dh = null_diagnostic_handler{};
    auto result
      = pipe_.inner.infer_type(tag_v<void>, base_ctx{null_dh, ctx.reg()});
    if (not result) {
      diagnostic::error("pipeline inside `each` must be a source")
        .primary(pipe_.source)
        .emit(ctx);
      return failure::promise();
    }
    if (input.is<table_slice>()) {
      if (*result == tag_v<table_slice>) {
        return tag_v<table_slice>;
      }
      if (*result == tag_v<void>) {
        return tag_v<void>;
      }
    } else {
      if (*result == tag_v<nova::Events>) {
        return tag_v<nova::Events>;
      }
      if (*result == tag_v<table_slice>) {
        diagnostic::error("pipeline inside Nova `each` must produce Nova "
                          "events")
          .primary(pipe_.source)
          .emit(ctx);
        return failure::promise();
      }
      if (*result == tag_v<void>) {
        return tag_v<void>;
      }
    }
    diagnostic::error("pipeline inside `each` must not produce bytes")
      .primary(pipe_.source)
      .emit(ctx);
    return failure::promise();
  }

  auto spawn(element_type_tag input, registry const& reg) const
    -> AnyOperator override {
    auto null_dh = null_diagnostic_handler{};
    auto output = pipe_.inner.infer_type(tag_v<void>, base_ctx{null_dh, reg});
    TENZIR_ASSERT(output);
    auto args = EachArgs{bindings_, pipe_, parallel_};
    if (input.is<nova::Events>()) {
      if (output->is<void>()) {
        return EachSinkNova{std::move(args)}.with_name("each");
      }
      TENZIR_ASSERT(output->is<nova::Events>());
      return EachNova{std::move(args)}.with_name("each");
    }
    TENZIR_ASSERT(input.is<table_slice>());
    if (output->is<void>()) {
      return EachSink{std::move(args)}.with_name("each");
    }
    TENZIR_ASSERT(output->is<table_slice>());
    return Each{std::move(args)}.with_name("each");
  }

  auto optimize(ir::OptimizeRequest req,
                ir::OptimizeCtx const& octx) && -> ir::OptimizeResult override {
    // The subpipeline produces our output from its own source, so downstream
    // filters, limits, and projections apply to the subpipeline rather than
    // our input events.
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
    pipe_.inner.prepend(std::move(sub.filter), octx.reg);
    // Upstream only needs the fields read by our binding expressions. The
    // default `$this=this` binding resets the projection to `None`, retaining
    // every input field.
    auto projection = Option<ir::OptimizeProjection>{ir::OptimizeProjection{}};
    for (auto const& binding : bindings_) {
      ir::add_refs_to_projection(projection, binding.expr);
    }
    auto replacement = ir::pipeline{};
    replacement.operators.push_back(std::move(*this).move());
    return {
      .filter = {},
      .order = EventOrder::ordered,
      .replacement = std::move(replacement),
      .limit = None{},
      .projection = std::move(projection),
    };
  }

  friend auto inspect(auto& f, EachIr& x) -> bool {
    return f.object(x).fields(f.field("self", x.self_),
                              f.field("bindings", x.bindings_),
                              f.field("parallel_expr", x.parallel_expr_),
                              f.field("parallel", x.parallel_),
                              f.field("pipe", x.pipe_));
  }

private:
  location self_;
  std::vector<EachBinding> bindings_;
  Option<ast::expression> parallel_expr_;
  uint64_t parallel_ = 10;
  located<ir::pipeline> pipe_;
};

struct ParsedBinding {
  std::string name;
  ast::expression expr;
};

class EachPlugin final : public virtual operator_compiler_plugin {
public:
  auto name() const -> std::string override {
    return "each";
  }

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<ir::CompileResult> override {
    // We use `operator_compiler_plugin` rather than `OperatorPlugin`/`Describer`
    // because named bindings (`each $a=expr1, $b=expr2 { ... }`) introduce a
    // variable number of dynamic `let` bindings into the subpipeline scope.
    constexpr auto usage = "each [$name=<expr>, ...] [parallel=<int>] { ... }";
    constexpr auto docs = "https://tenzir.com/docs/reference/operators/each";
    auto loc = inv.op.get_location();
    auto parsed_bindings = std::vector<ParsedBinding>{};
    auto seen_names = std::unordered_map<std::string, location>{};
    auto parallel_expr = Option<ast::expression>{};
    auto parallel_loc = Option<location>{};
    auto pipe_expr = Option<ast::pipeline_expr>{};
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
      if (auto* assignment = try_as<ast::assignment>(arg)) {
        if (auto* var = try_as<ast::dollar_var>(assignment->left)) {
          auto name = std::string{var->name_without_dollar()};
          auto name_loc = var->get_location();
          if (auto it = seen_names.find(name); it != seen_names.end()) {
            diagnostic::error("duplicate binding `${}`", name)
              .primary(name_loc)
              .secondary(it->second, "previously bound here")
              .emit(ctx);
            failed = true;
            continue;
          }
          seen_names.emplace(name, name_loc);
          parsed_bindings.push_back(ParsedBinding{
            .name = std::move(name),
            .expr = std::move(assignment->right),
          });
          continue;
        }
        if (auto* rf = try_as<ast::root_field>(assignment->left);
            rf and not rf->has_question_mark) {
          if (rf->id.name == "parallel") {
            if (parallel_expr) {
              diagnostic::error("duplicate argument `parallel`")
                .primary(arg)
                .secondary(*parallel_loc, "previously provided here")
                .emit(ctx);
              failed = true;
              continue;
            }
            parallel_loc = arg.get_location();
            parallel_expr = std::move(assignment->right);
            continue;
          }
          diagnostic::error("unknown argument `{}`", rf->id.name)
            .primary(assignment->left)
            .hint("use `${}=...` to bind `${}` in the subpipeline, or "
                  "`parallel=<int>` to set concurrency",
                  rf->id.name, rf->id.name)
            .docs(docs)
            .emit(ctx);
          failed = true;
          continue;
        }
        diagnostic::error("expected variable name")
          .primary(assignment->left)
          .hint("use a variable name such as `$x`")
          .docs(docs)
          .emit(ctx);
        failed = true;
        continue;
      }
      diagnostic::error("unexpected positional argument")
        .primary(arg)
        .hint("use `$name=<expr>` to bind a variable in the subpipeline")
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
    // Bind binding expressions and `parallel` against the enclosing scope
    // before opening the subpipeline scope.
    for (auto& binding : parsed_bindings) {
      TRY(binding.expr.bind(ctx));
    }
    if (parallel_expr) {
      TRY(parallel_expr->bind(ctx));
    }
    auto scope = ctx.open_scope();
    auto bindings = std::vector<EachBinding>{};
    if (parsed_bindings.empty()) {
      auto id = scope.let("this");
      bindings.push_back(EachBinding{
        .expr = ast::this_{loc},
        .let = id,
      });
    } else {
      bindings.reserve(parsed_bindings.size());
      for (auto& binding : parsed_bindings) {
        auto id = scope.let(std::move(binding.name));
        bindings.push_back(EachBinding{
          .expr = std::move(binding.expr),
          .let = id,
        });
      }
    }
    auto pipe_loc = pipe_expr->get_location();
    TRY(auto pipe_ir, std::move(pipe_expr->inner).compile(ctx));
    return EachIr{
      loc,
      std::move(bindings),
      std::move(parallel_expr),
      located{std::move(pipe_ir), pipe_loc},
    };
  }
};

} // namespace

} // namespace tenzir::plugins::each

TENZIR_REGISTER_PLUGIN(tenzir::plugins::each::EachPlugin)
TENZIR_REGISTER_PLUGIN(
  tenzir::inspection_plugin<tenzir::ir::Operator, tenzir::plugins::each::EachIr>)
