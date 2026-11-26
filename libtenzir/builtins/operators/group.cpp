//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
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
#include <tenzir/view3.hpp>

#include <arrow/compute/api.h>

#include <limits>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace tenzir::plugins::group {

namespace {

struct GroupBinding {
  ast::expression expr;
  let_id let;

  friend auto inspect(auto& f, GroupBinding& x) -> bool {
    return f.object(x).fields(f.field("expr", x.expr), f.field("let", x.let));
  }
};

struct GroupArgs {
  std::vector<GroupBinding> bindings;
  located<ir::pipeline> pipe;
};

struct RowGroup {
  data key;
  std::vector<int64_t> rows;
};

auto constant_from_key(data const& key) -> ast::constant::kind {
  return match(
    key,
    [](pattern const&) -> ast::constant::kind {
      TENZIR_UNREACHABLE();
    },
    []<class T>(T const& value) -> ast::constant::kind {
      return value;
    });
}

auto bind_group_key(ir::pipeline& pipe,
                    std::vector<GroupBinding> const& bindings, data const& key)
  -> void {
  TENZIR_ASSERT(not bindings.empty());
  if (bindings.size() == 1) {
    pipe.bind(bindings.front().let, constant_from_key(key));
    return;
  }
  auto const& values = as<list>(key);
  TENZIR_ASSERT(values.size() == bindings.size());
  for (auto i = size_t{0}; i < bindings.size(); ++i) {
    pipe.bind(bindings[i].let, constant_from_key(values[i]));
  }
}

auto make_row_groups(std::vector<multi_series> const& columns, int64_t rows)
  -> std::vector<RowGroup> {
  TENZIR_ASSERT(not columns.empty());
  auto result = std::vector<RowGroup>{};
  auto group_lookup = std::unordered_map<data, size_t>{};
  for (auto row = int64_t{0}; row < rows; ++row) {
    auto key = data{};
    if (columns.size() == 1) {
      key = materialize(columns.front().view3_at(row));
    } else {
      auto values = list{};
      values.reserve(columns.size());
      for (auto const& column : columns) {
        values.push_back(materialize(column.view3_at(row)));
      }
      key = std::move(values);
    }
    auto [it, inserted] = group_lookup.try_emplace(key, result.size());
    if (inserted) {
      result.push_back(RowGroup{std::move(key), {}});
    }
    result[it->second].rows.push_back(row);
  }
  return result;
}

class GroupBase {
public:
  explicit GroupBase(GroupArgs args) : args_{std::move(args)} {
  }

protected:
  auto snapshot_impl(Serde& serde) -> void {
    serde("seen_keys", seen_keys_);
  }

  auto process_impl(table_slice input, OpCtx& ctx) -> Task<void> {
    auto columns = std::vector<multi_series>{};
    columns.reserve(args_.bindings.size());
    for (auto const& binding : args_.bindings) {
      columns.push_back(eval(binding.expr, input, ctx));
    }
    auto groups
      = make_row_groups(columns, detail::narrow<int64_t>(input.rows()));
    for (auto& group : groups) {
      auto sub_slice = take_rows(input, group.rows);
      auto sub = Option<AnySubHandle&>{};
      if (seen_keys_.contains(group.key)) {
        sub = ctx.get_sub(make_view(group.key));
        if (not sub) {
          continue;
        }
      } else {
        auto copy = args_.pipe.inner;
        bind_group_key(copy, args_.bindings, group.key);
        sub = co_await ctx.plan_and_spawn_sub<table_slice>(group.key,
                                                           std::move(copy));
        if (not sub) {
          continue;
        }
        seen_keys_.emplace(group.key);
      }
      TENZIR_ASSERT(sub);
      std::ignore
        = co_await as<SubHandle<table_slice>>(*sub).push(std::move(sub_slice));
    }
  }

  GroupArgs args_;
  std::unordered_set<data> seen_keys_;
};

template <class Output>
class Group;

template <>
class Group<table_slice> final : public Operator<table_slice, table_slice>,
                                 private GroupBase {
public:
  explicit Group(GroupArgs args) : GroupBase{std::move(args)} {
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push);
    co_await process_impl(std::move(input), ctx);
  }

  auto snapshot(Serde& serde) -> void override {
    snapshot_impl(serde);
  }
};

template <>
class Group<void> final : public Operator<table_slice, void>,
                          private GroupBase {
public:
  explicit Group(GroupArgs args) : GroupBase{std::move(args)} {
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    co_await process_impl(std::move(input), ctx);
  }

  auto snapshot(Serde& serde) -> void override {
    snapshot_impl(serde);
  }
};

struct NovaGroup {
  data key;
  std::vector<nova::storage::Index> rows;
};

class GroupNovaBase {
public:
  explicit GroupNovaBase(GroupArgs args) : args_{std::move(args)} {
  }

  auto start_impl(OpCtx& ctx) -> Task<void> {
    evaluators_.reserve(args_.bindings.size());
    for (auto& binding : args_.bindings) {
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

  auto process_impl(nova::Events input, OpCtx& ctx) -> Task<void> {
    if (failed_ or not input.mask.any()) {
      co_return;
    }
    TENZIR_ASSERT(evaluators_.size() == args_.bindings.size());
    auto columns = std::vector<nova::Array<nova::Data>>{};
    columns.reserve(evaluators_.size());
    for (auto& evaluator : evaluators_) {
      columns.push_back(evaluator.eval(input, nova::EvalCtx{ctx.dh()}));
    }
    auto groups = std::vector<NovaGroup>{};
    auto group_lookup = std::unordered_map<data, size_t>{};
    for (auto row : nova::storage::true_bits(input.mask)) {
      auto key = data{};
      if (columns.size() == 1) {
        key = nova::materialize_legacy(columns.front().get(row));
      } else {
        auto values = list{};
        values.reserve(columns.size());
        for (auto const& column : columns) {
          values.push_back(nova::materialize_legacy(column.get(row)));
        }
        key = std::move(values);
      }
      auto [it, inserted] = group_lookup.try_emplace(key, groups.size());
      if (inserted) {
        groups.push_back(NovaGroup{std::move(key), {}});
      }
      groups[it->second].rows.push_back(row);
    }
    for (auto& group : groups) {
      auto sub = Option<AnySubHandle&>{};
      if (seen_keys_.contains(group.key)) {
        sub = ctx.get_sub(make_view(group.key));
        if (not sub) {
          continue;
        }
      } else {
        auto copy = args_.pipe.inner;
        bind_group_key(copy, args_.bindings, group.key);
        sub = co_await ctx.plan_and_spawn_sub<nova::Events>(group.key,
                                                            std::move(copy));
        if (not sub) {
          continue;
        }
        seen_keys_.emplace(group.key);
      }
      TENZIR_ASSERT(sub);
      auto mask = nova::storage::BitMap::Mutable{input.length()};
      for (auto row : group.rows) {
        mask.set(row, true);
      }
      auto events
        = nova::Events{input.data, std::move(mask).finish(), input.meta};
      std::ignore
        = co_await as<SubHandle<nova::Events>>(*sub).push(std::move(events));
    }
  }

  auto snapshot_impl(Serde& serde) -> void {
    serde("seen_keys", seen_keys_);
  }

  GroupArgs args_;
  std::vector<nova::Evaluator> evaluators_;
  std::unordered_set<data> seen_keys_;
  bool failed_ = false;
};

class GroupEvents final : public Operator<nova::Events, nova::Events>,
                          private GroupNovaBase {
public:
  explicit GroupEvents(GroupArgs args) : GroupNovaBase{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    return start_impl(ctx);
  }

  auto process(nova::Events input, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push);
    return process_impl(std::move(input), ctx);
  }

  auto snapshot(Serde& serde) -> void override {
    snapshot_impl(serde);
  }
};

class GroupEventsSink final : public Operator<nova::Events, void>,
                              private GroupNovaBase {
public:
  explicit GroupEventsSink(GroupArgs args) : GroupNovaBase{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    return start_impl(ctx);
  }

  auto process(nova::Events input, OpCtx& ctx) -> Task<void> override {
    return process_impl(std::move(input), ctx);
  }

  auto snapshot(Serde& serde) -> void override {
    snapshot_impl(serde);
  }
};

class GroupIr final : public ir::Operator {
public:
  GroupIr() = default;

  GroupIr(location self, std::vector<GroupBinding> bindings,
          located<ir::pipeline> pipe)
    : self_{self}, bindings_{std::move(bindings)}, pipe_{std::move(pipe)} {
  }

  auto name() const -> std::string override {
    return "group";
  }

  auto main_location() const -> location override {
    return self_;
  }

  auto copy() const -> Box<ir::Operator> override {
    return GroupIr{*this};
  }

  auto move() && -> Box<ir::Operator> override {
    return GroupIr{std::move(*this)};
  }

  auto concurrent_output() const -> bool override {
    return true;
  }

  auto max_parallelism() const -> size_t override {
    return std::numeric_limits<size_t>::max();
  }

  auto partition_keys() const -> std::vector<ast::expression> override {
    auto result = std::vector<ast::expression>{};
    result.reserve(bindings_.size());
    for (auto const& binding : bindings_) {
      result.push_back(binding.expr);
    }
    return result;
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    TENZIR_UNUSED(instantiate);
    for (auto& binding : bindings_) {
      TRY(binding.expr.substitute(ctx));
    }
    // The subpipeline is instantiated per group key at runtime, once its
    // group bindings are known.
    return pipe_.inner.substitute(ctx, false);
  }

  auto infer_type(element_type_tag input, base_ctx ctx) const
    -> failure_or<element_type_tag> override {
    if (input.is_not<nova::Events>() and input.is_not<table_slice>()) {
      diagnostic::error("operator expects events").primary(self_).emit(ctx);
      return failure::promise();
    }
    TRY(auto output, pipe_.inner.infer_type(input, ctx));
    if (input.is<table_slice>()) {
      return match(
        output,
        [](tag<table_slice>) -> failure_or<element_type_tag> {
          return tag_v<table_slice>;
        },
        [](tag<void>) -> failure_or<element_type_tag> {
          return tag_v<void>;
        },
        [&](tag<chunk_ptr>) -> failure_or<element_type_tag> {
          diagnostic::error("subpipeline must not produce bytes")
            .primary(pipe_.source)
            .emit(ctx);
          return failure::promise();
        },
        [&](tag<nova::Events>) -> failure_or<element_type_tag> {
          diagnostic::error("subpipeline must not produce nova_events")
            .primary(pipe_.source)
            .emit(ctx);
          return failure::promise();
        },
        [](tag<FileHandle>) -> failure_or<element_type_tag> {
          // Files only feed readers, so no subpipeline produces them.
          TENZIR_UNREACHABLE();
        });
    }
    return match(
      output,
      [](tag<nova::Events>) -> failure_or<element_type_tag> {
        return tag_v<nova::Events>;
      },
      [](tag<void>) -> failure_or<element_type_tag> {
        return tag_v<void>;
      },
      [&](tag<chunk_ptr>) -> failure_or<element_type_tag> {
        diagnostic::error("subpipeline must not produce bytes")
          .primary(pipe_.source)
          .emit(ctx);
        return failure::promise();
      },
      [&](tag<table_slice>) -> failure_or<element_type_tag> {
        diagnostic::error("subpipeline must not produce legacy events")
          .primary(pipe_.source)
          .emit(ctx);
        return failure::promise();
      },
      [](tag<FileHandle>) -> failure_or<element_type_tag> {
        // Files only feed readers, so no subpipeline produces them.
        TENZIR_UNREACHABLE();
      });
  }

  auto spawn(element_type_tag input, registry const& reg) const
    -> AnyOperator override {
    auto noop_dh = null_diagnostic_handler{};
    auto output = pipe_.inner.infer_type(input, base_ctx{noop_dh, reg});
    TENZIR_ASSERT(output);
    auto args = GroupArgs{bindings_, pipe_};
    if (input.is<nova::Events>()) {
      if (output->is<void>()) {
        return GroupEventsSink{std::move(args)}.with_name("group");
      }
      TENZIR_ASSERT(output->is<nova::Events>());
      return GroupEvents{std::move(args)}.with_name("group");
    }
    TENZIR_ASSERT(input.is<table_slice>());
    if (output->is<void>()) {
      return Group<void>{std::move(args)}.with_name("group");
    }
    TENZIR_ASSERT(output->is<table_slice>());
    return Group<table_slice>{std::move(args)}.with_name("group");
  }

  auto optimize(ir::OptimizeRequest req,
                ir::OptimizeCtx const& octx) && -> ir::OptimizeResult override {
    // The subpipeline produces our output, so it can consume downstream hints.
    // Repeated subpipelines may overproduce, while downstream operators still
    // enforce the global result.
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
    // Predicates that stay inside the subpipeline still read upstream fields.
    for (auto const& expr : sub.filter) {
      ir::add_refs_to_projection(sub.projection, expr);
    }
    pipe_.inner.prepend(std::move(sub.filter), octx.reg);
    for (auto const& binding : bindings_) {
      ir::add_refs_to_projection(sub.projection, binding.expr);
    }
    auto replacement = ir::pipeline{};
    replacement.operators.push_back(std::move(*this).move());
    return {
      .filter = {},
      .order = EventOrder::ordered,
      .replacement = std::move(replacement),
      .limit = None{},
      .projection = std::move(sub.projection),
    };
  }

  friend auto inspect(auto& f, GroupIr& x) -> bool {
    return f.object(x).fields(f.field("self", x.self_),
                              f.field("bindings", x.bindings_),
                              f.field("pipe", x.pipe_));
  }

private:
  location self_;
  std::vector<GroupBinding> bindings_;
  located<ir::pipeline> pipe_;
};

struct ParsedKey {
  Option<std::string> name;
  ast::expression expr;
};

class group_plugin final : public virtual operator_compiler_plugin {
public:
  auto name() const -> std::string override {
    return "group";
  }

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<ir::CompileResult> override {
    // We use `operator_compiler_plugin` rather than `OperatorPlugin`/`Describer`
    // because named bindings (`group $a=expr1, $b=expr2 { ... }`) introduce a
    // variable number of dynamic `let` bindings into the subpipeline scope.
    constexpr auto usage
      = "group <expr> { ... } or group $name=<expr>, ... { ... }";
    constexpr auto docs = "https://tenzir.com/docs/reference/operators/group";
    auto loc = inv.op.get_location();
    auto keys = std::vector<ParsedKey>{};
    auto pipe_expr = Option<ast::pipeline_expr>{};
    auto seen_names = std::unordered_map<std::string, location>{};
    auto unnamed_loc = Option<location>{};
    auto named_loc = Option<location>{};
    auto saw_key_arg = false;
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
      saw_key_arg = true;
      if (auto* assignment = try_as<ast::assignment>(arg)) {
        if (auto* var = try_as<ast::dollar_var>(assignment->left)) {
          auto name = std::string{var->name_without_dollar()};
          auto name_loc = var->get_location();
          if (auto it = seen_names.find(name); it != seen_names.end()) {
            diagnostic::error("duplicate group binding `${}`", name)
              .primary(name_loc)
              .secondary(it->second, "previously bound here")
              .emit(ctx);
            failed = true;
            continue;
          }
          if (unnamed_loc) {
            diagnostic::error("cannot mix named and unnamed group keys")
              .primary(arg)
              .secondary(*unnamed_loc, "unnamed key provided here")
              .hint("name every key with `$name=<expr>`")
              .docs(docs)
              .emit(ctx);
            failed = true;
            continue;
          }
          seen_names.emplace(name, name_loc);
          if (not named_loc) {
            named_loc = arg.get_location();
          }
          keys.push_back(ParsedKey{
            .name = std::move(name),
            .expr = std::move(assignment->right),
          });
          continue;
        }
        if (auto* rf = try_as<ast::root_field>(assignment->left);
            rf and not rf->has_question_mark) {
          if (rf->id.name == "over") {
            if (named_loc) {
              diagnostic::error("cannot mix named and unnamed group keys")
                .primary(arg)
                .secondary(*named_loc, "named binding provided here")
                .hint("name every key with `$name=<expr>`")
                .docs(docs)
                .emit(ctx);
              failed = true;
              continue;
            }
            if (unnamed_loc) {
              diagnostic::error("multiple group keys must be named")
                .primary(arg)
                .secondary(*unnamed_loc, "first key provided here")
                .hint("bind each key to a variable, as in "
                      "`group $a=..., $b=... { ... }`")
                .docs(docs)
                .emit(ctx);
              failed = true;
              continue;
            }
            unnamed_loc = arg.get_location();
            keys.push_back(ParsedKey{
              .name = None{},
              .expr = std::move(assignment->right),
            });
            continue;
          }
          diagnostic::error("group binding must start with `$`")
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
          .hint("use a variable name such as `$key`")
          .docs(docs)
          .emit(ctx);
        failed = true;
        continue;
      }
      if (named_loc) {
        diagnostic::error("cannot mix named and unnamed group keys")
          .primary(arg)
          .secondary(*named_loc, "named binding provided here")
          .hint("name every key with `$name=<expr>`")
          .docs(docs)
          .emit(ctx);
        failed = true;
        continue;
      }
      if (unnamed_loc) {
        diagnostic::error("multiple group keys must be named")
          .primary(arg)
          .secondary(*unnamed_loc, "first key provided here")
          .hint("bind each key to a variable, as in "
                "`group $a=..., $b=... { ... }`")
          .docs(docs)
          .emit(ctx);
        failed = true;
        continue;
      }
      unnamed_loc = arg.get_location();
      keys.push_back(ParsedKey{
        .name = None{},
        .expr = std::move(arg),
      });
    }
    if (not saw_key_arg) {
      diagnostic::error("expected at least one group key")
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
    // Bind key expressions against the enclosing scope before opening the
    // subpipeline scope, so `let $x = 1h; group $x=ts.floor($x) { ... }`
    // resolves `$x` on the RHS to the outer binding and `$x` inside the
    // subpipeline to the group binding.
    for (auto& key : keys) {
      TRY(key.expr.bind(ctx));
    }
    auto scope = ctx.open_scope();
    auto bindings = std::vector<GroupBinding>{};
    bindings.reserve(keys.size());
    for (auto& key : keys) {
      auto var_name = key.name ? std::move(*key.name) : std::string{"group"};
      auto id = scope.let(std::move(var_name));
      bindings.push_back(GroupBinding{
        .expr = std::move(key.expr),
        .let = id,
      });
    }
    auto pipe_loc = pipe_expr->get_location();
    TRY(auto pipe_ir, std::move(pipe_expr->inner).compile(ctx));
    return GroupIr{
      loc,
      std::move(bindings),
      located{std::move(pipe_ir), pipe_loc},
    };
  }
};

} // namespace

} // namespace tenzir::plugins::group

TENZIR_REGISTER_PLUGIN(tenzir::plugins::group::group_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::inspection_plugin<
                       tenzir::ir::Operator, tenzir::plugins::group::GroupIr>)
