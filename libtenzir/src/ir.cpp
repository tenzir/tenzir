//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/ir.hpp"

#include "tenzir/async.hpp"
#include "tenzir/compile_ctx.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/rebatch.hpp"
#include "tenzir/session.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/tql2/resolve.hpp"
#include "tenzir/tql2/set.hpp"
#include "tenzir/tql2/user_defined_operator.hpp"
#include "tenzir/view3.hpp"

#include <ranges>

namespace tenzir {

namespace {} // namespace

auto make_where_ir(ast::expression filter) -> Box<ir::Operator> {
  // TODO: This should just be a `where_ir{std::move(filter)}`.
  const auto* where = plugins::find<operator_compiler_plugin>("tql2.where");
  TENZIR_ASSERT(where);
  auto args = std::vector<ast::expression>{};
  args.push_back(std::move(filter));
  // TODO: This is a terrible workaround. We are discarding diagnostics and
  // creating a new compile context, which should be created only once.
  auto dh = null_diagnostic_handler{};
  auto reg = global_registry();
  auto ctx = compile_ctx::make_root(base_ctx{dh, *reg});
  auto compiled
    = where->compile(ast::invocation{ast::entity{{}}, std::move(args)}, ctx)
        .unwrap();
  auto pipe = std::move(compiled).unwrap();
  TENZIR_ASSERT(pipe.lets.empty());
  TENZIR_ASSERT_EQ(pipe.operators.size(), 1);
  return std::move(pipe.operators.front());
}

namespace {

// Migration hints for the neo executor transition.
struct porting_hint {
  std::string_view legacy_name;
  std::string_view message;
};

constexpr porting_hint unported_replacements[] = {
  {"compress", "use one of the `compress_*` operators (e.g. `compress_gzip`, "
               "`compress_zstd`) instead"},
  {"decompress", "use one of the `decompress_*` operators (e.g. "
                 "`decompress_gzip`, "
                 "`decompress_zstd`) instead"},
  {"from", "use one of the `from_*` operators (e.g. `from_file`, "
           "`from_http`) "
           "instead"},
  {"from_gcs", "use `from_google_cloud_storage` instead"},
  {"from_udp", "use `accept_udp` instead"},
  {"http", "use `from_http` instead, combined with `each` if needed"},
  {"load_amqp", "use `from_amqp` instead"},
  {"load_azure_blob_storage", "use `from_azure_blob_storage` instead"},
  {"load_gcs", "use `from_google_cloud_storage` instead"},
  {"load_google_cloud_pubsub", "use `from_google_cloud_pubsub` instead"},
  {"load_kafka", "use `from_kafka` instead"},
  {"load_nic", "use `from_nic` instead"},
  {"load_s3", "use `from_s3` instead"},
  {"load_sqs", "use `from_sqs` instead"},
  {"load_tcp", "use `accept_tcp` instead"},
  {"load_zmq", "use `from_zmq` instead"},
  {"move", "use the `dst = move src` keyword form instead"},
  {"save_amqp", "use `to_amqp` instead"},
  {"save_azure_blob_storage", "use `to_azure_blob_storage` instead"},
  {"save_gcs", "use `to_google_cloud_storage` instead"},
  {"save_google_cloud_pubsub", "use `to_google_cloud_pubsub` instead"},
  {"save_kafka", "use `to_kafka` instead"},
  {"save_s3", "use `to_s3` instead"},
  {"save_sqs", "use `to_sqs` instead"},
  {"save_zmq", "use `to_zmq` instead"},
  {"to", "use one of the `to_*` operators (e.g. `to_file`, `to_http`) "
         "instead"},
  {"to_hive", "use `to_file`, `to_s3`, etc. with hive partitioning instead"},
};

auto get_porting_hint(const ast::entity& op) -> std::string_view {
  const auto it = std::ranges::find(unported_replacements, op.path.back().name,
                                    &porting_hint::legacy_name);
  return it != std::ranges::end(unported_replacements) ? it->message
                                                       : std::string_view{};
}

auto merge_compiled_pipeline(std::vector<ir::let>& lets,
                             std::vector<Box<ir::Operator>>& operators,
                             ir::pipeline pipe) -> void {
  lets.insert(lets.end(), std::move_iterator{pipe.lets.begin()},
              std::move_iterator{pipe.lets.end()});
  operators.insert(operators.end(), std::move_iterator{pipe.operators.begin()},
                   std::move_iterator{pipe.operators.end()});
}

class Set final : public Operator<table_slice, table_slice> {
public:
  Set(std::vector<ast::assignment> assignments, event_order order)
    : assignments_{std::move(assignments)}, order_{order} {
    for (auto& assignment : assignments_) {
      auto [pruned_assignment, moved_fields]
        = resolve_move_keyword(std::move(assignment));
      assignment = std::move(pruned_assignment);
      std::ranges::move(moved_fields, std::back_inserter(moved_fields_));
    }
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> {
    auto slice = std::move(input);
    // The right-hand side is always evaluated with the original input, because
    // side-effects from preceding assignments shall not be reflected when
    // calculating the value of the left-hand side.
    auto values = std::vector<multi_series>{};
    for (const auto& assignment : assignments_) {
      values.push_back(eval(assignment.right, slice, ctx));
    }
    slice = drop(slice, moved_fields_, ctx, false);
    // After we know all the multi series values on the right, we can split the
    // input table slice and perform the actual assignment.
    auto begin = int64_t{0};
    auto results = std::vector<table_slice>{};
    for (auto values_slice : split_multi_series(values)) {
      TENZIR_ASSERT(not values_slice.empty());
      auto end = begin + values_slice[0].length();
      // We could still perform further splits if metadata is assigned.
      auto state = std::vector<table_slice>{};
      state.push_back(subslice(slice, begin, end));
      begin = end;
      auto new_state = std::vector<table_slice>{};
      for (auto [assignment, value] :
           std::views::zip(assignments_, values_slice)) {
        auto begin = int64_t{0};
        for (auto& entry : state) {
          auto entry_rows = detail::narrow<int64_t>(entry.rows());
          auto assigned = assign(assignment.left,
                                 value.slice(begin, entry_rows), entry, ctx);
          begin += entry_rows;
          new_state.insert(new_state.end(),
                           std::move_iterator{assigned.begin()},
                           std::move_iterator{assigned.end()});
        }
        std::swap(state, new_state);
        new_state.clear();
      }
      std::ranges::move(state, std::back_inserter(results));
    }
    // TODO: Consider adding a property to function plugins that let's them
    // indicate whether they want their outputs to be strictly ordered. If any
    // of the called functions has this requirement, then we should not be
    // making this optimization. This will become relevant in the future once we
    // allow functions to be stateful.
    if (order_ != event_order::ordered) {
      std::ranges::stable_sort(results, std::ranges::less{},
                               &table_slice::schema);
    }
    for (auto& result : rebatch(std::move(results))) {
      co_await push(std::move(result));
    }
  }

private:
  std::vector<ast::assignment> assignments_;
  event_order order_{};
  std::vector<ast::field_path> moved_fields_;
};

} // namespace

ir::SetIr::SetIr() : order_{event_order::ordered} {
}

ir::SetIr::SetIr(std::vector<ast::assignment> assignments)
  : assignments_{std::move(assignments)}, order_{event_order::ordered} {
}

auto ir::SetIr::name() const -> std::string {
  return "SetIr";
}

auto ir::SetIr::substitute(substitute_ctx ctx, bool instantiate)
  -> failure_or<void> {
  (void)instantiate;
  for (auto& x : assignments_) {
    TRY(x.right.substitute(ctx));
  }
  return {};
}

auto ir::SetIr::spawn(element_type_tag input) && -> AnyOperator {
  TENZIR_ASSERT(input.is<table_slice>());
  return Set{std::move(assignments_), order_};
}

auto ir::SetIr::optimize(ir::optimize_filter filter,
                         event_order order) && -> ir::optimize_result {
  order_ = weaker_event_order(order_, order);
  auto ops = std::vector<Box<ir::Operator>>{};
  ops.reserve(1 + filter.size());
  ops.emplace_back(ir::SetIr{std::move(*this)});
  for (auto& expr : filter) {
    ops.push_back(make_where_ir(expr));
  }
  return {
    ir::optimize_filter{},
    order_,
    ir::pipeline{{}, std::move(ops)},
  };
}

auto ir::SetIr::infer_type(element_type_tag input, diagnostic_handler& dh) const
  -> failure_or<std::optional<element_type_tag>> {
  if (input.is_not<table_slice>()) {
    diagnostic::error("set operator expected events").emit(dh);
    return failure::promise();
  }
  return input;
}

namespace ir {

template <class Inspector>
auto inspect(Inspector& f, SetIr& x) -> bool {
  return f.object(x).fields(f.field("assignments", x.assignments_),
                            f.field("order", x.order_));
}

} // namespace ir

namespace {

/// Create a `set` operator with the given assignment.
auto make_set_ir(ast::assignment x) -> Box<ir::Operator> {
  auto assignments = std::vector<ast::assignment>{};
  assignments.push_back(std::move(x));
  return ir::SetIr{std::move(assignments)};
}

struct MatchPattern {
  struct Wildcard {};
  struct Constant {
    data value;
  };
  struct Range {
    data lower;
    data upper;
  };
  struct Field {
    std::string name;
    Box<MatchPattern> pattern;
  };
  struct Record {
    std::vector<Field> fields;
    bool has_rest = false;
  };
  using kind_type = variant<Wildcard, Constant, Range, Record>;

  kind_type kind;

  friend auto inspect(auto& f, Wildcard& x) -> bool {
    TENZIR_UNUSED(x);
    return f.object(x).fields();
  }

  friend auto inspect(auto& f, Constant& x) -> bool {
    return f.object(x).fields(f.field("value", x.value));
  }

  friend auto inspect(auto& f, Range& x) -> bool {
    return f.object(x).fields(f.field("lower", x.lower),
                              f.field("upper", x.upper));
  }

  friend auto inspect(auto& f, Field& x) -> bool {
    return f.object(x).fields(f.field("name", x.name),
                              f.field("pattern", x.pattern));
  }

  friend auto inspect(auto& f, Record& x) -> bool {
    return f.object(x).fields(f.field("fields", x.fields),
                              f.field("has_rest", x.has_rest));
  }

  friend auto inspect(auto& f, MatchPattern& x) -> bool {
    return f.object(x).fields(f.field("kind", x.kind));
  }
};

struct MatchArgs {
  struct Arm {
    location source;
    std::vector<ast::match_pattern> pattern_exprs;
    std::vector<MatchPattern> patterns;
    ir::pipeline pipeline;
    bool wildcard = false;

    friend auto inspect(auto& f, Arm& x) -> bool {
      return f.object(x).fields(f.field("source", x.source),
                                f.field("pattern_exprs", x.pattern_exprs),
                                f.field("patterns", x.patterns),
                                f.field("pipeline", x.pipeline),
                                f.field("wildcard", x.wildcard));
    }
  };

  location match_keyword;
  ast::expression scrutinee;
  std::vector<Arm> arms;

  friend auto inspect(auto& f, MatchArgs& x) -> bool {
    return f.object(x).fields(f.field("match_keyword", x.match_keyword),
                              f.field("scrutinee", x.scrutinee),
                              f.field("arms", x.arms));
  }
};

auto matches_pattern(data_view3 value, MatchPattern const& pattern) -> bool;

struct IfArgs {
  struct Else {
    location else_keyword;
    ir::pipeline pipeline;

    friend auto inspect(auto& f, Else& x) -> bool {
      return f.object(x).fields(f.field("keyword", x.else_keyword),
                                f.field("pipeline", x.pipeline));
    }
  };

  location if_keyword;
  ast::expression condition;
  ir::pipeline consequence;
  std::optional<Else> alternative;

  friend auto inspect(auto& f, IfArgs& x) -> bool {
    return f.object(x).fields(f.field("if_keyword", x.if_keyword),
                              f.field("condition", x.condition),
                              f.field("consequence", x.consequence),
                              f.field("alternative", x.alternative));
  }
};

/// Shared implementation for both transform and sink variants of `if`.
class IfImpl {
public:
  explicit IfImpl(IfArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> {
    // Spawn subpipelines if they are not already spawned (due to restore).
    if (not ctx.get_sub(true).is_some()) {
      co_await ctx.spawn_sub<table_slice>(true, args_.consequence);
      if (args_.alternative) {
        co_await ctx.spawn_sub<table_slice>(false, args_.alternative->pipeline);
      }
    }
  }

  auto process(table_slice input, OpCtx& ctx, Push<table_slice>* push = nullptr)
    -> Task<void> {
    // FIXME: If the inner subpipelines terminate and get erased, this can fail.
    auto& true_sub = ctx.get_sub(true).unwrap();
    auto& consequence = as<SubHandle<table_slice>>(true_sub);
    auto false_sub = ctx.get_sub(false);
    auto alternative
      = false_sub ? Option<SubHandle<table_slice>&>{as<SubHandle<table_slice>>(
                      *false_sub)}
                  : None{};
    TENZIR_ASSERT(alternative.is_some() == args_.alternative.has_value());
    auto true_events = std::vector<table_slice>{};
    auto false_events = std::vector<table_slice>{};
    auto end = int64_t{0};
    for (auto const& predicate : eval(args_.condition, input, ctx)) {
      auto const start = std::exchange(end, end + predicate.length());
      TENZIR_ASSERT(end > start);
      auto const sliced_input = subslice(input, start, end);
      auto const typed_predicate = predicate.as<bool_type>();
      if (not typed_predicate) {
        diagnostic::warning("expected `bool`, but got `{}`",
                            predicate.type.kind())
          .primary(args_.condition)
          .emit(ctx);
        TENZIR_ASSERT(sliced_input.rows() > 0);
        false_events.push_back(sliced_input);
        continue;
      }
      if (typed_predicate->array->null_count() > 0) {
        diagnostic::warning("expected `bool`, but got `null`")
          .primary(args_.condition)
          .emit(ctx);
      }
      auto [lhs, rhs] = partition(sliced_input, *typed_predicate->array);
      TENZIR_ASSERT(lhs.rows() + rhs.rows() == sliced_input.rows());
      if (lhs.rows() > 0) {
        true_events.push_back(std::move(lhs));
      }
      if (rhs.rows() > 0) {
        false_events.push_back(std::move(rhs));
      }
    }
    if (not consequence_closed_) {
      for (auto& slice : rebatch(std::move(true_events))) {
        consequence_closed_
          = (co_await consequence.push(std::move(slice))).is_err();
      }
    }
    if (not alternative_closed_) {
      for (auto& slice : rebatch(std::move(false_events))) {
        if (alternative) {
          alternative_closed_
            = (co_await alternative->push(std::move(slice))).is_err();
        } else if (push) {
          co_await (*push)(std::move(slice));
        }
      }
    }
  }

  auto state() -> OperatorState {
    if (consequence_closed_ and alternative_closed_) {
      return OperatorState::done;
    }
    return OperatorState::normal;
  }

private:
  IfArgs args_;
  bool consequence_closed_ = false;
  bool alternative_closed_ = false;
};

class MatchImpl {
public:
  explicit MatchImpl(MatchArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> {
    for (auto i = size_t{0}; i < args_.arms.size(); ++i) {
      if (not args_.arms[i].pipeline.operators.empty()
          and ctx.get_sub(static_cast<int64_t>(i)).is_some()) {
        co_return;
      }
    }
    for (auto i = size_t{0}; i < args_.arms.size(); ++i) {
      if (args_.arms[i].pipeline.operators.empty()) {
        continue;
      }
      co_await ctx.spawn_sub<table_slice>(static_cast<int64_t>(i),
                                          args_.arms[i].pipeline);
    }
  }

  auto process(table_slice input, OpCtx& ctx, Push<table_slice>* push = nullptr)
    -> Task<void> {
    auto arm_masks = std::vector<std::vector<bool>>(args_.arms.size());
    for (auto& mask : arm_masks) {
      mask.resize(input.rows(), false);
    }
    auto matched = std::vector<bool>(input.rows(), false);
    auto scrutinee = eval(args_.scrutinee, input, ctx);
    auto offset = int64_t{0};
    for (auto& part : scrutinee) {
      for (auto value : part.values3()) {
        auto row = offset++;
        for (auto arm_index = size_t{0}; arm_index < args_.arms.size();
             ++arm_index) {
          if (matched[row]) {
            break;
          }
          auto const& arm = args_.arms[arm_index];
          auto matches = arm.wildcard;
          for (auto const& pattern : arm.patterns) {
            if (matches_pattern(value, pattern)) {
              matches = true;
              break;
            }
          }
          if (matches) {
            arm_masks[arm_index][row] = true;
            matched[row] = true;
          }
        }
      }
    }
    TENZIR_ASSERT_EQ(offset, static_cast<int64_t>(input.rows()));
    for (auto arm_index = size_t{0}; arm_index < args_.arms.size();
         ++arm_index) {
      if (arm_closed_[arm_index]) {
        continue;
      }
      auto builder = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
      check(builder.Reserve(input.rows()));
      for (auto value : arm_masks[arm_index]) {
        builder.UnsafeAppend(value);
      }
      auto filtered = filter(input, *finish(builder));
      if (filtered.rows() == 0) {
        continue;
      }
      auto sub = ctx.get_sub(static_cast<int64_t>(arm_index));
      if (not sub) {
        if (args_.arms[arm_index].pipeline.operators.empty()) {
          if (push) {
            co_await (*push)(std::move(filtered));
          }
        } else {
          arm_closed_[arm_index] = true;
        }
        continue;
      }
      auto& handle = as<SubHandle<table_slice>>(*sub);
      arm_closed_[arm_index]
        = (co_await handle.push(std::move(filtered))).is_err();
    }
    if (not has_wildcard() and push) {
      auto builder = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
      check(builder.Reserve(input.rows()));
      for (auto value : matched) {
        builder.UnsafeAppend(not value);
      }
      auto filtered = filter(input, *finish(builder));
      if (filtered.rows() > 0) {
        co_await (*push)(std::move(filtered));
      }
    }
  }

  auto state() -> OperatorState {
    // Without a wildcard, unmatched rows pass through even after all explicit
    // arms have closed, so upstream must continue.
    if (not has_wildcard()) {
      return OperatorState::normal;
    }
    if (std::ranges::all_of(arm_closed_, std::identity{})) {
      return OperatorState::done;
    }
    return OperatorState::normal;
  }

private:
  auto has_wildcard() const -> bool {
    return std::ranges::any_of(args_.arms, &MatchArgs::Arm::wildcard);
  }

  MatchArgs args_;
  std::vector<bool> arm_closed_ = std::vector<bool>(args_.arms.size(), false);
};

class Match final : public Operator<table_slice, table_slice> {
public:
  explicit Match(MatchArgs args) : impl_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    return impl_.start(ctx);
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    return impl_.process(std::move(input), ctx, &push);
  }

  auto state() -> OperatorState override {
    return impl_.state();
  }

private:
  MatchImpl impl_;
};

class MatchSink final : public Operator<table_slice, void> {
public:
  explicit MatchSink(MatchArgs args) : impl_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    return impl_.start(ctx);
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    return impl_.process(std::move(input), ctx);
  }

  auto state() -> OperatorState override {
    return impl_.state();
  }

private:
  MatchImpl impl_;
};

class If final : public Operator<table_slice, table_slice> {
public:
  explicit If(IfArgs args) : impl_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    return impl_.start(ctx);
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    return impl_.process(std::move(input), ctx, &push);
  }

  auto state() -> OperatorState override {
    return impl_.state();
  }

private:
  IfImpl impl_;
};

/// Sink variant of `if` for when both branches return void.
class IfSink final : public Operator<table_slice, void> {
public:
  explicit IfSink(IfArgs args) : impl_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    return impl_.start(ctx);
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    return impl_.process(std::move(input), ctx);
  }

  auto state() -> OperatorState override {
    return impl_.state();
  }

private:
  IfImpl impl_;
};

} // namespace

auto make_set_ir(std::vector<ast::assignment> assignments)
  -> Box<ir::Operator> {
  return ir::SetIr{std::move(assignments)};
}

namespace {

auto const_eval_match_expression(ast::expression const& expr, location source,
                                 diagnostic_handler& dh) -> failure_or<data> {
  auto diagnostics = collecting_diagnostic_handler{};
  auto value = const_eval(expr, diagnostics);
  if (value.is_error() or not diagnostics.empty()) {
    diagnostic::error("match patterns must be constant expressions")
      .primary(source)
      .hint("use a literal, a constant `let` binding, or `_`")
      .emit(dh);
    return failure::promise();
  }
  return *value;
}

using BindingMap = std::unordered_map<std::string, ast::expression>;
using BindingPath = std::vector<ast::identifier>;

auto is_irrefutable_match_pattern(ast::match_pattern const& pattern,
                                  compile_ctx const& ctx) -> bool;

auto validate_match_pattern_bindings(ast::match_pattern const& pattern,
                                     compile_ctx const& ctx,
                                     std::unordered_set<std::string>& names)
  -> failure_or<void>;

auto bind_match_pattern(ast::match_pattern& pattern, compile_ctx& ctx)
  -> failure_or<void>;

auto collect_match_pattern_bindings(ast::match_pattern const& pattern,
                                    ast::expression const& scrutinee,
                                    BindingPath& path, compile_ctx const& ctx,
                                    BindingMap& replacements)
  -> failure_or<void>;

auto compare_range_bounds(data const& lower, data const& upper)
  -> std::partial_ordering;

auto validate_record_pattern_scrutinee(ast::match_pattern const& pattern,
                                       ast::expression const& scrutinee,
                                       diagnostic_handler& dh)
  -> failure_or<void>;

auto substitute_match_pattern(ast::match_pattern& pattern, substitute_ctx ctx,
                              bool instantiate) -> failure_or<void>;

auto lower_match_pattern(ast::match_pattern const& pattern,
                         diagnostic_handler& dh) -> failure_or<MatchPattern>;

auto is_irrefutable_match_pattern(ast::match_pattern const& pattern,
                                  compile_ctx const& ctx) -> bool {
  return pattern.kind->match<bool>(
    [](ast::wildcard_pattern const&) {
      return true;
    },
    [](ast::expression_pattern const&) {
      return false;
    },
    [&](ast::binding_pattern const& binding) {
      auto name = std::string_view{binding.name.name};
      TENZIR_ASSERT(name.starts_with("$"));
      return not ctx.get(name.substr(1));
    },
    [](ast::range_pattern const&) {
      return false;
    },
    [](ast::record_pattern const&) {
      return false;
    });
}

auto validate_match_pattern_bindings(ast::match_pattern const& pattern,
                                     compile_ctx const& ctx,
                                     std::unordered_set<std::string>& names)
  -> failure_or<void> {
  return pattern.kind->match<failure_or<void>>(
    [](ast::wildcard_pattern const&) -> failure_or<void> {
      return {};
    },
    [](ast::expression_pattern const&) -> failure_or<void> {
      return {};
    },
    [&](ast::binding_pattern const& binding) -> failure_or<void> {
      auto name = std::string_view{binding.name.name};
      TENZIR_ASSERT(name.starts_with("$"));
      if (ctx.get(name.substr(1))) {
        return {};
      }
      if (not names.emplace(binding.name.name).second) {
        diagnostic::error("binding `{}` appears more than once in this match "
                          "arm",
                          binding.name.name)
          .primary(binding)
          .emit(ctx);
        return failure::promise();
      }
      return {};
    },
    [&](ast::range_pattern const&) -> failure_or<void> {
      return {};
    },
    [&](ast::record_pattern const& record) -> failure_or<void> {
      for (auto const& field : record.fields) {
        TRY(validate_match_pattern_bindings(*field.pattern, ctx, names));
      }
      return {};
    });
}

auto bind_match_pattern(ast::match_pattern& pattern, compile_ctx& ctx)
  -> failure_or<void> {
  return pattern.kind->match<failure_or<void>>(
    [](ast::wildcard_pattern&) -> failure_or<void> {
      return {};
    },
    [&](ast::expression_pattern& expr) -> failure_or<void> {
      return expr.expr.bind(ctx);
    },
    [&](ast::binding_pattern& binding) -> failure_or<void> {
      auto name = std::string_view{binding.name.name};
      TENZIR_ASSERT(name.starts_with("$"));
      if (ctx.get(name.substr(1))) {
        auto expr = ast::expression{ast::dollar_var{binding.name}};
        TRY(expr.bind(ctx));
        pattern.kind = Box<ast::match_pattern_kind>{
          std::in_place,
          ast::expression_pattern{std::move(expr)},
        };
      }
      return {};
    },
    [&](ast::range_pattern& range) -> failure_or<void> {
      TRY(range.lower.bind(ctx));
      TRY(range.upper.bind(ctx));
      return {};
    },
    [&](ast::record_pattern& record) -> failure_or<void> {
      for (auto& field : record.fields) {
        TRY(bind_match_pattern(*field.pattern, ctx));
      }
      return {};
    });
}

auto is_addressable_scrutinee(ast::expression const& scrutinee) -> bool {
  if (std::holds_alternative<ast::this_>(*scrutinee.kind)) {
    return true;
  }
  auto copy = scrutinee;
  return ast::field_path::try_from(std::move(copy)).has_value();
}

auto make_binding_expression(ast::expression const& scrutinee,
                             BindingPath const& path) -> ast::expression {
  auto result = scrutinee;
  for (auto const& field : path) {
    result = ast::expression{ast::field_access{
      std::move(result),
      field.location,
      false,
      field,
    }};
  }
  return result;
}

auto collect_match_pattern_bindings(ast::match_pattern const& pattern,
                                    ast::expression const& scrutinee,
                                    BindingPath& path, compile_ctx const& ctx,
                                    BindingMap& replacements)
  -> failure_or<void> {
  return pattern.kind->match<failure_or<void>>(
    [](ast::wildcard_pattern const&) -> failure_or<void> {
      return {};
    },
    [](ast::expression_pattern const&) -> failure_or<void> {
      return {};
    },
    [&](ast::binding_pattern const& binding) -> failure_or<void> {
      auto name = std::string_view{binding.name.name};
      TENZIR_ASSERT(name.starts_with("$"));
      if (ctx.get(name.substr(1))) {
        return {};
      }
      auto const addressable_scrutinee = is_addressable_scrutinee(scrutinee);
      if (not path.empty() and not addressable_scrutinee) {
        diagnostic::error("binding `{}` cannot be derived from a "
                          "non-addressable match expression",
                          binding.name.name)
          .primary(binding)
          .emit(ctx);
        return failure::promise();
      }
      if (not addressable_scrutinee
          and not scrutinee.is_deterministic(ctx.reg())) {
        diagnostic::error("binding `{}` depends on a non-deterministic match "
                          "expression",
                          binding.name.name)
          .primary(binding)
          .emit(ctx);
        return failure::promise();
      }
      replacements.emplace(std::string{name.substr(1)},
                           make_binding_expression(scrutinee, path));
      return {};
    },
    [](ast::range_pattern const&) -> failure_or<void> {
      return {};
    },
    [&](ast::record_pattern const& record) -> failure_or<void> {
      for (auto const& field : record.fields) {
        path.push_back(field.name);
        TRY(collect_match_pattern_bindings(*field.pattern, scrutinee, path, ctx,
                                           replacements));
        path.pop_back();
      }
      return {};
    });
}

auto validate_record_pattern_scrutinee(ast::match_pattern const& pattern,
                                       ast::expression const& scrutinee,
                                       diagnostic_handler& dh)
  -> failure_or<void> {
  if (not std::holds_alternative<ast::record_pattern>(*pattern.kind)) {
    return {};
  }
  auto diagnostics = collecting_diagnostic_handler{};
  auto value = const_eval(scrutinee, diagnostics);
  if (value.is_error() or not diagnostics.empty()) {
    return {};
  }
  if (not std::holds_alternative<record>(value->get_data())) {
    diagnostic::error("record pattern cannot match non-record expression")
      .primary(pattern)
      .emit(dh);
    return failure::promise();
  }
  return {};
}

auto substitute_match_expression(ast::expression& expr, location source,
                                 substitute_ctx ctx, bool instantiate)
  -> failure_or<void> {
  TRY(auto subst, expr.substitute(ctx));
  if (instantiate and subst == ast::substitute_result::some_remaining) {
    diagnostic::error("match patterns must be constant expressions")
      .primary(source)
      .emit(ctx);
    return failure::promise();
  }
  return {};
}

auto substitute_match_pattern(ast::match_pattern& pattern, substitute_ctx ctx,
                              bool instantiate) -> failure_or<void> {
  return pattern.kind->match<failure_or<void>>(
    [](ast::wildcard_pattern&) -> failure_or<void> {
      return {};
    },
    [&](ast::expression_pattern& expr) -> failure_or<void> {
      return substitute_match_expression(expr.expr, expr.get_location(), ctx,
                                         instantiate);
    },
    [](ast::binding_pattern&) -> failure_or<void> {
      return {};
    },
    [&](ast::range_pattern& range) -> failure_or<void> {
      TRY(substitute_match_expression(range.lower, range.lower.get_location(),
                                      ctx, instantiate));
      TRY(substitute_match_expression(range.upper, range.upper.get_location(),
                                      ctx, instantiate));
      return {};
    },
    [&](ast::record_pattern& record) -> failure_or<void> {
      for (auto& field : record.fields) {
        TRY(substitute_match_pattern(*field.pattern, ctx, instantiate));
      }
      return {};
    });
}

auto compare_range_bounds(data const& lower, data const& upper)
  -> std::partial_ordering {
  return lower.get_data().match<std::partial_ordering>(
    [&](caf::none_t value) {
      return partial_order(data_view3{value}, upper);
    },
    [&](bool value) {
      return partial_order(data_view3{value}, upper);
    },
    [&](int64_t value) {
      return partial_order(data_view3{value}, upper);
    },
    [&](uint64_t value) {
      return partial_order(data_view3{value}, upper);
    },
    [&](double value) {
      return partial_order(data_view3{value}, upper);
    },
    [&](duration value) {
      return partial_order(data_view3{value}, upper);
    },
    [&](time value) {
      return partial_order(data_view3{value}, upper);
    },
    [&](std::string const& value) {
      return partial_order(data_view3{value}, upper);
    },
    [](pattern const&) {
      return std::partial_ordering::unordered;
    },
    [&](ip value) {
      return partial_order(data_view3{value}, upper);
    },
    [&](subnet value) {
      return partial_order(data_view3{value}, upper);
    },
    [&](enumeration value) {
      return partial_order(data_view3{value}, upper);
    },
    [](list const&) {
      return std::partial_ordering::unordered;
    },
    [](map const&) {
      return std::partial_ordering::unordered;
    },
    [](record const&) {
      return std::partial_ordering::unordered;
    },
    [&](blob const& value) {
      return partial_order(data_view3{value}, upper);
    },
    [&](secret const& value) {
      return partial_order(data_view3{value}, upper);
    });
}

auto lower_match_pattern(ast::record_pattern const& pattern,
                         diagnostic_handler& dh) -> failure_or<MatchPattern> {
  auto fields = std::vector<MatchPattern::Field>{};
  for (auto const& field : pattern.fields) {
    TRY(auto nested, lower_match_pattern(*field.pattern, dh));
    fields.push_back(
      MatchPattern::Field{field.name.name, Box{std::move(nested)}});
  }
  return MatchPattern{
    MatchPattern::Record{std::move(fields), pattern.rest.is_some()}};
}

auto lower_match_pattern(ast::match_pattern const& pattern,
                         diagnostic_handler& dh) -> failure_or<MatchPattern> {
  return pattern.kind->match<failure_or<MatchPattern>>(
    [](ast::wildcard_pattern const&) -> failure_or<MatchPattern> {
      return MatchPattern{MatchPattern::Wildcard{}};
    },
    [&](ast::expression_pattern const& expr) -> failure_or<MatchPattern> {
      TRY(auto value,
          const_eval_match_expression(expr.expr, expr.get_location(), dh));
      return MatchPattern{MatchPattern::Constant{std::move(value)}};
    },
    [](ast::binding_pattern const&) -> failure_or<MatchPattern> {
      return MatchPattern{MatchPattern::Wildcard{}};
    },
    [&](ast::range_pattern const& range) -> failure_or<MatchPattern> {
      TRY(auto lower, const_eval_match_expression(
                        range.lower, range.lower.get_location(), dh));
      TRY(auto upper, const_eval_match_expression(
                        range.upper, range.upper.get_location(), dh));
      if (compare_range_bounds(lower, upper)
          == std::partial_ordering::unordered) {
        diagnostic::error("range pattern bounds are not comparable")
          .primary(range.dots)
          .emit(dh);
        return failure::promise();
      }
      return MatchPattern{
        MatchPattern::Range{std::move(lower), std::move(upper)}};
    },
    [&](ast::record_pattern const& record) -> failure_or<MatchPattern> {
      return lower_match_pattern(record, dh);
    });
}

auto matches_record(data_view3 value, MatchPattern::Record const& pattern)
  -> bool {
  auto const* record = std::get_if<record_view3>(&value);
  if (not record) {
    return false;
  }
  auto matched_fields = size_t{0};
  for (auto const& field : pattern.fields) {
    auto found = false;
    for (auto const& [name, nested] : *record) {
      if (name == field.name) {
        found = matches_pattern(nested, *field.pattern);
        break;
      }
    }
    if (not found) {
      return false;
    }
    ++matched_fields;
  }
  auto field_count = size_t{0};
  for ([[maybe_unused]] auto const& field : *record) {
    ++field_count;
  }
  return pattern.has_rest or field_count == matched_fields;
}

auto matches_pattern(data_view3 value, MatchPattern const& pattern) -> bool {
  if (std::holds_alternative<MatchPattern::Wildcard>(pattern.kind)) {
    return true;
  }
  if (auto constant = std::get_if<MatchPattern::Constant>(&pattern.kind)) {
    return partial_order(value, constant->value)
           == std::partial_ordering::equivalent;
  }
  if (auto range = std::get_if<MatchPattern::Range>(&pattern.kind)) {
    auto lower = partial_order(value, range->lower);
    auto upper = partial_order(value, range->upper);
    return lower != std::partial_ordering::less
           and lower != std::partial_ordering::unordered
           and upper != std::partial_ordering::greater
           and upper != std::partial_ordering::unordered;
  }
  auto const* record = std::get_if<MatchPattern::Record>(&pattern.kind);
  TENZIR_ASSERT(record);
  return matches_record(value, *record);
}

} // namespace

auto combine_branch_types(std::optional<element_type_tag> lhs,
                          std::optional<element_type_tag> rhs, location primary,
                          diagnostic_handler& dh)
  -> failure_or<std::optional<element_type_tag>> {
  if (not lhs) {
    return rhs;
  }
  if (not rhs) {
    return lhs;
  }
  if (*lhs == *rhs) {
    return lhs;
  }
  if (lhs->is<void>()) {
    return rhs;
  }
  if (rhs->is<void>()) {
    return lhs;
  }
  diagnostic::error("incompatible branch output types: {} and {}",
                    operator_type_name(*lhs), operator_type_name(*rhs))
    .primary(primary)
    .emit(dh);
  return failure::promise();
}

class MatchIr final : public ir::Operator {
public:
  MatchIr() = default;

  explicit MatchIr(MatchArgs args) : args_{std::move(args)} {
  }

  auto name() const -> std::string override {
    return "Match";
  }

  auto copy() const -> Box<ir::Operator> override {
    return MatchIr{args_};
  }

  auto move() && -> Box<ir::Operator> override {
    return MatchIr{std::move(args_)};
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    TRY(args_.scrutinee.substitute(ctx));
    for (auto& arm : args_.arms) {
      arm.patterns.clear();
      for (auto& pattern : arm.pattern_exprs) {
        TRY(substitute_match_pattern(pattern, ctx, instantiate));
        if (instantiate) {
          TRY(auto lowered, lower_match_pattern(pattern, ctx));
          arm.patterns.push_back(std::move(lowered));
        }
      }
      TRY(arm.pipeline.substitute(ctx, instantiate));
    }
    return {};
  }

  auto spawn(element_type_tag input) && -> AnyOperator override {
    TENZIR_ASSERT(input.is<table_slice>());
    auto dh = null_diagnostic_handler{};
    auto output = infer_type(input, dh);
    TENZIR_ASSERT(output and *output);
    if ((**output).is<void>()) {
      return MatchSink{std::move(args_)};
    }
    return Match{std::move(args_)};
  }

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<std::optional<element_type_tag>> override {
    if (input.is_not<table_slice>()) {
      diagnostic::error("match operator expected events").emit(dh);
      return failure::promise();
    }
    auto result = std::optional<element_type_tag>{};
    auto has_wildcard = false;
    for (auto const& arm : args_.arms) {
      has_wildcard = has_wildcard or arm.wildcard;
      TRY(auto branch_ty, arm.pipeline.infer_type(input, dh));
      if (branch_ty and branch_ty->is<chunk_ptr>()) {
        diagnostic::error("branches must not return bytes")
          .primary(arm.source)
          .emit(dh);
        return failure::promise();
      }
      TRY(result,
          combine_branch_types(result, branch_ty, args_.match_keyword, dh));
    }
    if (not has_wildcard) {
      TRY(result, combine_branch_types(result, std::optional{input},
                                       args_.match_keyword, dh));
    }
    return result;
  }

  friend auto inspect(auto& f, MatchIr& x) -> bool {
    return f.apply(x.args_);
  }

private:
  MatchArgs args_;
};

class IfIr final : public ir::Operator {
public:
  IfIr() = default;

  explicit IfIr(IfArgs args) : args_{std::move(args)} {
  }

  auto name() const -> std::string override {
    return "If";
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    TRY(args_.condition.substitute(ctx));
    TRY(args_.consequence.substitute(ctx, instantiate));
    if (args_.alternative) {
      TRY(args_.alternative->pipeline.substitute(ctx, instantiate));
    }
    return {};
  }

  auto optimize(ir::optimize_filter filter,
                event_order order) && -> ir::optimize_result override {
    // We need to skip `-> void` pipelines, which are invalid to optimize with
    // the downstream filter.
    auto null_dh = null_diagnostic_handler{};
    auto outputs_events = [&](ir::pipeline const& pipe) -> bool {
      auto t = pipe.infer_type(tag_v<table_slice>, null_dh);
      return t and *t and (**t).is<table_slice>();
    };
    auto optimize_branch
      = [&](ir::pipeline& branch, ir::optimize_filter f) -> event_order {
      auto opt = std::move(branch).optimize(std::move(f), order);
      branch = std::move(opt.replacement);
      branch.operators.insert_range(branch.operators.begin(),
                                    opt.filter
                                      | std::views::transform(make_where_ir));
      return opt.order;
    };
    // Handle downstream filters when there is no explicit `else` branch.
    if (not args_.alternative and not filter.empty()) {
      args_.alternative.emplace(IfArgs::Else{location{}, ir::pipeline{}});
    }
    auto cons_filter
      = outputs_events(args_.consequence) ? filter : ir::optimize_filter{};
    auto cons_order
      = optimize_branch(args_.consequence, std::move(cons_filter));
    auto alt_order = order;
    if (args_.alternative) {
      auto alt_filter = outputs_events(args_.alternative->pipeline)
                          ? std::move(filter)
                          : ir::optimize_filter{};
      alt_order
        = optimize_branch(args_.alternative->pipeline, std::move(alt_filter));
    }
    auto replacement = std::vector<Box<ir::Operator>>{};
    replacement.push_back(std::move(*this).move());
    return {
      {},
      stronger_event_order(cons_order, alt_order),
      ir::pipeline{{}, std::move(replacement)},
    };
  }

  auto spawn(element_type_tag input) && -> AnyOperator override {
    TENZIR_ASSERT(input.is<table_slice>());
    auto dh = null_diagnostic_handler{};
    auto output = infer_type(input, dh);
    TENZIR_ASSERT(output and *output);
    if ((**output).is<void>()) {
      return IfSink{std::move(args_)};
    }
    return If{std::move(args_)};
  }

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<std::optional<element_type_tag>> override {
    TRY(auto then_ty, args_.consequence.infer_type(input, dh));
    auto else_ty = std::optional{input};
    if (args_.alternative) {
      TRY(else_ty, args_.alternative->pipeline.infer_type(input, dh));
    }
    if (then_ty and then_ty->is<chunk_ptr>()) {
      diagnostic::error("branches must not return bytes")
        .primary(args_.if_keyword)
        .emit(dh);
      return failure::promise();
    }
    if (args_.alternative and else_ty and else_ty->is<chunk_ptr>()) {
      diagnostic::error("branches must not return bytes")
        .primary(args_.alternative->else_keyword)
        .emit(dh);
      return failure::promise();
    }
    if (not then_ty) {
      return else_ty;
    }
    if (not else_ty) {
      return then_ty;
    }
    if (*then_ty == *else_ty) {
      return then_ty;
    }
    if (then_ty->is<void>()) {
      return else_ty;
    }
    if (else_ty->is<void>()) {
      return then_ty;
    }
    // TODO: Improve diagnostic.
    diagnostic::error("incompatible branch output types: {} and {}",
                      operator_type_name(*then_ty),
                      operator_type_name(*else_ty))
      .primary(args_.if_keyword)
      .emit(dh);
    return failure::promise();
  }

  friend auto inspect(auto& f, IfIr& x) -> bool {
    return f.apply(x.args_);
  }

private:
  IfArgs args_;
};

namespace {

// TODO: Clean this up. We might want to be able to just use
// `TENZIR_REGISTER_PLUGINS` also from `libtenzir` itself.
auto register_plugins_somewhat_hackily = std::invoke([]() {
  auto x = std::initializer_list<plugin*>{
    new inspection_plugin<ir::Operator, IfIr>{},
    new inspection_plugin<ir::Operator, MatchIr>{},
    new inspection_plugin<ir::Operator, ir::SetIr>{},
  };
  for (auto y : x) {
    auto ptr = plugin_ptr::make_builtin(y,
                                        [](plugin* plugin) {
                                          delete plugin;
                                        },
                                        nullptr, {});
    const auto it = std::ranges::upper_bound(plugins::get_mutable(), ptr);
    plugins::get_mutable().insert(it, std::move(ptr));
  }
  return std::monostate{};
});

} // namespace

ir::CompileResult::CompileResult(Box<ir::Operator> op) {
  pipeline_.operators.push_back(std::move(op));
}

ir::CompileResult::CompileResult(pipeline pipe) : pipeline_{std::move(pipe)} {
}

auto ir::CompileResult::unwrap() && -> pipeline {
  return std::move(pipeline_);
}

auto ast::pipeline::compile(compile_ctx ctx) && -> failure_or<ir::pipeline> {
  // TODO: Or do we assume that entities are already resolved?
  TRY(resolve_entities(*this, ctx));
  auto lets = std::vector<ir::let>{};
  auto operators = std::vector<Box<ir::Operator>>{};
  auto scope = ctx.open_scope();
  for (auto& stmt : body) {
    auto result = match(
      std::move(stmt),
      [&](ast::invocation x) -> failure_or<void> {
        auto& op = ctx.reg().get(x);
        return match(
          op.inner(),
          [&](const native_operator& op) -> failure_or<void> {
            if (not op.ir_plugin) {
// FIXME: Decider whether to make a hard cut or not.
#if 0
              TENZIR_ASSERT(op.factory_plugin);
              for (auto& x : x.args) {
                // TODO: This doesn't work for operators which take
                // subpipelines... Should we just disallow subpipelines here?
                TRY(x.bind(ctx));
              }
              operators.emplace_back(
                legacy_ir{op.factory_plugin, std::move(x))};
              // TODO: Empty substitution?
              TRY(operators.back()->substitute(substitute_ctx{ctx, nullptr},
                                               false));
              return {};
#else
              diagnostic::error("This operator is not available in Tenzir v6")
                .primary(x.op)
                .hint("{}", get_porting_hint(x.op))
                .hint("see https://docs.tenzir.com/guides/tenzir-v6-migration")
                .emit(ctx);
              return failure::promise();
#endif
            }
            // If there is a pipeline argument, we can't resolve `let`s in there
            // because the operator might introduce its own bindings. Thus, we
            // do not resolve any bindings, even when not in subpipelines. This
            // also gives the operator the option to accept let-bindings that
            // were not defined, for example because it can then introduce those
            // bindings by itself.
            TRY(auto compiled, op.ir_plugin->compile(x, ctx));
            merge_compiled_pipeline(lets, operators,
                                    std::move(compiled).unwrap());
            return {};
          },
          [&](const user_defined_operator& op) -> failure_or<void> {
            auto op_name = make_operator_name(x.op);
            auto udo_dh = udo_diagnostic_handler{
              &static_cast<diagnostic_handler&>(ctx), op_name, op};
            // Bind argument expressions in the outer ctx so that any
            // `$outer_let` references are resolved here.
            for (auto& arg : x.args) {
              if (auto* assignment = try_as<ast::assignment>(arg)) {
                TRY(assignment->right.bind(ctx));
              } else {
                TRY(arg.bind(ctx));
              }
            }
            // Validate args and substitute them into the body AST. The
            // session adopts `udo_dh` so that diagnostics also carry the
            // call-site usage and parameters.
            auto sp = session_provider::make(udo_dh);
            auto inv
              = operator_factory_invocation{std::move(x.op), std::move(x.args)};
            TRY(auto substituted, instantiate_user_defined_operator(
                                    op, inv, sp.as_session(), udo_dh));
            // The body is hygienic: it cannot see outer `let` bindings. Any
            // outer references reach the body only through arguments, which
            // we pre-bound above before substitution copied them in.
            auto udo_ctx = ctx.without_env();
            TRY(auto pipe, std::move(substituted).compile(udo_ctx));
            merge_compiled_pipeline(lets, operators, std::move(pipe));
            return {};
          });
      },
      [&](ast::assignment x) -> failure_or<void> {
        // TODO: What about left?
        TRY(x.right.bind(ctx));
        operators.push_back(make_set_ir(std::move(x)));
        return {};
      },
      [&](ast::let_stmt x) -> failure_or<void> {
        TRY(x.expr.bind(ctx));
        auto id = scope.let(std::string{x.name_without_dollar()});
        lets.emplace_back(std::move(x.name), std::move(x.expr), id);
        return {};
      },
      [&](ast::if_stmt x) -> failure_or<void> {
        TRY(x.condition.bind(ctx));
        TRY(auto then, std::move(x.then).compile(ctx));
        auto args = IfArgs{};
        args.if_keyword = x.if_kw;
        args.condition = std::move(x.condition);
        args.consequence = std::move(then);
        if (x.else_) {
          TRY(auto pipe, std::move(x.else_->pipe).compile(ctx));
          args.alternative.emplace(x.else_->kw, std::move(pipe));
        }
        operators.emplace_back(IfIr{std::move(args)});
        return {};
      },
      [&](ast::match_stmt x) -> failure_or<void> {
        TRY(x.expr.bind(ctx));
        auto args = MatchArgs{};
        args.match_keyword = x.begin;
        args.scrutinee = std::move(x.expr);
        for (auto arm_index = size_t{0}; auto& ast_arm : x.arms) {
          auto arm = MatchArgs::Arm{};
          arm.source = ast_arm.patterns.front().get_location();
          arm.wildcard
            = std::ranges::any_of(ast_arm.patterns, [&](auto& pattern) {
                return is_irrefutable_match_pattern(pattern, ctx);
              });
          if (arm.wildcard and arm_index + 1 != x.arms.size()) {
            diagnostic::error("irrefutable match arm must be last")
              .primary(arm.source)
              .emit(ctx);
            return failure::promise();
          }
          auto bindings = std::unordered_set<std::string>{};
          for (auto const& pattern : ast_arm.patterns) {
            TRY(validate_match_pattern_bindings(pattern, ctx, bindings));
            TRY(validate_record_pattern_scrutinee(pattern, args.scrutinee,
                                                  ctx));
          }
          if (not arm.wildcard) {
            for (auto& pattern : ast_arm.patterns) {
              TRY(bind_match_pattern(pattern, ctx));
              arm.pattern_exprs.push_back(pattern);
            }
          }
          auto replacements = BindingMap{};
          auto path = BindingPath{};
          for (auto const& pattern : ast_arm.patterns) {
            TRY(collect_match_pattern_bindings(pattern, args.scrutinee, path,
                                               ctx, replacements));
          }
          if (not replacements.empty()) {
            TRY(ast_arm.pipe, substitute_named_expressions(
                                std::move(ast_arm.pipe), replacements, ctx));
          }
          TRY(arm.pipeline, std::move(ast_arm.pipe).compile(ctx));
          args.arms.push_back(std::move(arm));
          ++arm_index;
        }
        operators.emplace_back(MatchIr{std::move(args)});
        return {};
      },
      [&](ast::type_stmt x) -> failure_or<void> {
        diagnostic::error(
          "type declarations are not yet supported within pipelines")
          .primary(x.type_location)
          .emit(ctx);
        return failure::promise();
      });
    TRY(result);
  }
  return ir::pipeline{std::move(lets), std::move(operators)};
}

auto ir::pipeline::substitute(substitute_ctx ctx, bool instantiate)
  -> failure_or<void> {
  if (instantiate) {
    auto env = ctx.env();
    for (auto& let : lets) {
      // We have to update every expression as we evaluate `let`s because later
      // bindings might reference earlier ones.
      TRY(auto subst, let.expr.substitute(ctx.with_env(&env)));
      TENZIR_ASSERT(subst == ast::substitute_result::no_remaining);
      TRY(auto value, const_eval(let.expr, ctx));
      // TODO: Clean this up. Should probably make `const_eval` return it.
      auto converted = match(
        value,
        [](auto& x) -> ast::constant::kind {
          return std::move(x);
        },
        [](pattern&) -> ast::constant::kind {
          TENZIR_UNREACHABLE();
        });
      auto inserted = env.try_emplace(let.id, std::move(converted)).second;
      TENZIR_ASSERT(inserted);
    }
    // Update each operator with the produced bindings.
    for (auto& op : operators) {
      TRY(op->substitute(ctx.with_env(&env), true));
    }
    // We don't need the lets anymore.
    lets.clear();
    return {};
  }
  // TODO: Do we still want to substitute deterministic bindings in here? Or
  // should that happen somewhere else? Could also help with type-checking.
  for (auto& let : lets) {
    TRY(let.expr.substitute(ctx));
  }
  for (auto& op : operators) {
    TRY(op->substitute(ctx, false));
  }
  return {};
}

auto ir::pipeline::spawn(element_type_tag input) && -> std::vector<AnyOperator> {
  // TODO: Assert that we were instantiated, or instantiate ourselves?
  TENZIR_ASSERT(lets.empty());
  // TODO: This is probably not the right place for optimizations.
  auto opt = std::move(*this).optimize(optimize_filter{}, event_order::ordered);
  TENZIR_ASSERT(opt.replacement.lets.empty());
  // TODO: Should we really ignore this here?
  (void)opt.order;
  for (auto& expr : opt.filter) {
    opt.replacement.operators.insert(opt.replacement.operators.begin(),
                                     make_where_ir(expr));
  }
  *this = std::move(opt.replacement);
  auto result = std::vector<AnyOperator>{};
  for (auto& op : operators) {
    // We already checked, there should be no diagnostics here.
    auto dh = null_diagnostic_handler{};
    auto output = op->infer_type(input, dh);
    TENZIR_ASSERT(output);
    TENZIR_ASSERT(*output);
    result.push_back(std::move(*op).spawn(input));
    input = **output;
  }
  return result;
}

auto ir::pipeline::infer_type(element_type_tag input,
                              diagnostic_handler& dh) const
  -> failure_or<std::optional<element_type_tag>> {
  for (auto& op : operators) {
    TRY(auto output, op->infer_type(input, dh));
    TRY(input, output);
    // TODO: What if we get void in the middle?
  }
  return input;
}

auto ir::pipeline::optimize(optimize_filter filter,
                            event_order order) && -> optimize_result {
  auto replacement = pipeline{std::move(lets), {}};
  for (auto& op : std::ranges::reverse_view(operators)) {
    auto opt = std::move(*op).optimize(std::move(filter), order);
    filter = std::move(opt.filter);
    order = opt.order;
    replacement.operators.insert(
      replacement.operators.begin(),
      std::move_iterator{opt.replacement.operators.begin()},
      std::move_iterator{opt.replacement.operators.end()});
  }
  return {std::move(filter), order, std::move(replacement)};
}

auto ir::Operator::optimize(optimize_filter filter,
                            event_order order) && -> optimize_result {
  TENZIR_UNUSED(order);
  auto replacement = std::vector<Box<Operator>>{};
  replacement.push_back(std::move(*this).move());
  for (auto& expr : filter) {
    replacement.push_back(make_where_ir(std::move(expr)));
  }
  return {
    optimize_filter{},
    event_order::ordered,
    pipeline{{}, std::move(replacement)},
  };
}

auto ir::Operator::copy() const -> Box<Operator> {
  auto p = plugins::find<serialization_plugin<Operator>>(name());
  if (not p) {
    TENZIR_ERROR("could not find serialization plugin `{}`", name());
    TENZIR_ASSERT(false);
  }
  auto buffer = caf::byte_buffer{};
  auto f = caf::binary_serializer{buffer};
  auto success = p->serialize(f, *this);
  if (not success) {
    TENZIR_ERROR("failed to serialize `{}` operator: {}", name(),
                 f.get_error());
    TENZIR_ASSERT(false);
  }
  auto g = caf::binary_deserializer{buffer};
  auto copy = std::unique_ptr<ir::Operator>{};
  p->deserialize(g, copy);
  if (not copy) {
    TENZIR_ERROR("failed to deserialize `{}` operator: {}", name(),
                 g.get_error());
    TENZIR_ASSERT(false);
  }
  return Box<Operator>::from_non_null(std::move(copy));
}

auto ir::Operator::move() && -> Box<Operator> {
  // TODO: This should be overriden by something like CRTP.
  return copy();
}

auto ir::Operator::infer_type(element_type_tag input,
                              diagnostic_handler& dh) const
  -> failure_or<std::optional<element_type_tag>> {
  // TODO: Is this a good default to have? Should probably be pure virtual.
  (void)input, (void)dh;
  return std::nullopt;
}

auto operator_compiler_plugin::operator_name() const -> std::string {
  auto result = name();
  if (result.starts_with("tql2.")) {
    result = result.substr(5);
  }
  return result;
}

ir::pipeline::pipeline(std::vector<let> lets,
                       std::vector<Box<Operator>> operators)
  : lets{std::move(lets)}, operators{std::move(operators)} {
}

} // namespace tenzir
