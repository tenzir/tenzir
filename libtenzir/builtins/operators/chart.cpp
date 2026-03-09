//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arrow_time_utils.hpp"
#include "tenzir/arrow_utils.hpp"
#include "tenzir/async.hpp"
#include "tenzir/cast.hpp"
#include "tenzir/concept/printable/tenzir/json.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/resolve.hpp"

#include <ranges>

namespace tenzir::plugins::chart {

namespace {

TENZIR_ENUM(chart_type, area, bar, line, pie);

auto to_double(data d) -> data {
  return match(
    d,
    [](concepts::integer auto& value) -> data {
      return static_cast<double>(value);
    },
    [](auto& value) -> data {
      return std::move(value);
    });
}

auto jsonify_limit(const data& d) -> std::string {
  auto result = std::string{};
  const auto printer = tenzir::json_printer{tenzir::json_printer_options{
    .tql = true,
    .numeric_durations = true,
  }};
  auto it = std::back_inserter(result);
  TENZIR_ASSERT(printer.print(it, make_view_wrapper(d)));
  return result;
}

/// Extracts contiguous runs of `true` values from a boolean array and returns
/// the corresponding subslices.
auto split_contiguous_true_runs(const arrow::BooleanArray& array,
                                const table_slice& slice, int64_t offset = 0)
  -> std::vector<table_slice> {
  const auto len = array.length();
  // Fast path: no true values
  if (array.true_count() == 0) {
    return {};
  }
  // Fast path: all true values
  if (array.true_count() == len) {
    return {subslice(slice, offset, offset + len)};
  }
  // Extract contiguous runs of true values
  auto results = std::vector<table_slice>{};
  auto curr = array.Value(0);
  auto begin = int64_t{0};
  // We add an artificial `false` at index `length` to flush.
  for (auto i = int64_t{1}; i < len + 1; ++i) {
    const auto next = i != len && array.IsValid(i) && array.Value(i);
    if (curr == next) {
      continue;
    }
    if (curr) {
      results.push_back(subslice(slice, offset + begin, offset + i));
    }
    curr = next;
    begin = i;
  }
  return results;
}

using bucket = std::vector<std::unique_ptr<aggregation_instance>>;
// Will point to valid strings, as `std::unordered_set` does not invalidate
// pointers on insertion
using grouped_bucket = detail::stable_map<std::string_view, bucket>;
using group_map = std::map<data, grouped_bucket>;
using call_map = detail::stable_map<std::string, ast::function_call>;
using plugins_map
  = std::vector<std::pair<const aggregation_plugin*, ast::function_call>>;

template <chart_type Ty>
struct RawArgs {
  ast::field_path x;
  ast::expression y;
  std::optional<ast::expression> group;
  std::optional<located<data>> x_min;
  std::optional<located<data>> x_max;
  std::optional<located<data>> y_min;
  std::optional<located<data>> y_max;
  std::optional<located<duration>> res;
  std::optional<located<data>> fill;
  std::optional<location> x_log;
  std::optional<location> y_log;
  located<uint64_t> limit{
    Ty == chart_type::bar or Ty == chart_type::pie ? 100u : 100'000u,
    location::unknown,
  };
  located<std::string> position{"grouped", location::unknown};
};

struct xlimit {
  located<data> value;
  data rounded;
  expression legacy_expr;
  ast::expression expr;

  friend auto inspect(auto& f, xlimit& x) -> bool {
    return f.object(x).fields(f.field("value", x.value),
                              f.field("rounded", x.rounded),
                              f.field("legacy_expr", x.legacy_expr),
                              f.field("expr", x.expr));
  }
};

struct chart_args {
  chart_type ty;
  ast::field_path x;
  call_map y;
  std::optional<ast::expression> group;
  std::optional<xlimit> x_min;
  std::optional<xlimit> x_max;
  std::optional<located<data>> y_min;
  std::optional<located<data>> y_max;
  std::optional<located<duration>> res;
  std::optional<located<data>> fill;
  std::optional<location> x_log;
  std::optional<location> y_log;
  located<uint64_t> limit{100'000, location::unknown};
  located<std::string> position{"grouped", location::unknown};
  expression filter{trivially_true_expression()};
  location y_loc;
  location op_loc;

  friend auto inspect(auto& f, chart_args& x) -> bool {
    return f.object(x)
      .pretty_name("chart_args")
      .fields(f.field("ty", x.ty), f.field("x", x.x), f.field("y", x.y),
              f.field("group", x.group), f.field("x_min", x.x_min),
              f.field("x_max", x.x_max), f.field("y_min", x.y_min),
              f.field("y_max", x.y_max), f.field("res", x.res),
              f.field("fill", x.fill), f.field("x_log", x.x_log),
              f.field("y_log", x.y_log), f.field("limit", x.limit),
              f.field("filter", x.filter), f.field("position", x.position),
              f.field("y_loc", x.y_loc), f.field("op_loc", x.op_loc));
  }

  auto validate(diagnostic_handler& dh) const -> failure_or<void> {
    if (position.inner != "stacked" and position.inner != "grouped") {
      diagnostic::error("unsupported `position`")
        .primary(position)
        .hint("available positions: `grouped` (default) or `stacked`")
        .emit(dh);
      return failure::promise();
    }
    if (x_min) {
      TRY(validate_xtype(x_min->value, dh));
    }
    if (x_max) {
      TRY(validate_xtype(x_max->value, dh));
    }
    if (x_min and x_max) {
      const auto min_idx = x_min->value.inner.get_data().index();
      const auto max_idx = x_max->value.inner.get_data().index();
      if (min_idx != max_idx) {
        diagnostic::error("`x_min` and `x_max` must have the same type")
          .primary(x_min->value.source)
          .primary(x_max->value.source)
          .emit(dh);
        return failure::promise();
      }
      if (x_min->value.inner >= x_max->value.inner) {
        diagnostic::error("`x_min` must be less than `x_max`")
          .primary(x_min->value.source)
          .primary(x_max->value.source)
          .emit(dh);
        return failure::promise();
      }
    }
    if (y_min) {
      TRY(validate_ytype(*y_min, dh));
    }
    if (y_max) {
      TRY(validate_ytype(*y_max, dh));
    }
    if (y_min and y_max) {
      if (y_min->inner.get_data().index() != y_max->inner.get_data().index()) {
        diagnostic::error("`y_min` and `y_max` must have the same type")
          .primary(*y_min)
          .primary(*y_max)
          .emit(dh);
        return failure::promise();
      }
      if (y_min->inner >= y_max->inner) {
        diagnostic::error("`y_min` must be less than `y_max`")
          .primary(*y_min)
          .primary(*y_max)
          .emit(dh);
        return failure::promise();
      }
    }
    if (fill) {
      if (not res) {
        diagnostic::error("`fill` cannot be specified without `resolution`")
          .primary(fill->source)
          .emit(dh);
        return failure::promise();
      }
      TRY(validate_ytype(*fill, dh));
      if (y_min or y_max) {
        const auto& type_idx = y_min ? y_min->inner.get_data().index()
                                     : y_max->inner.get_data().index();
        if (type_idx != fill->inner.get_data().index()) {
          diagnostic::error("`fill` has a different type from `{}`",
                            y_min ? "y_min" : "y_max")
            .primary(fill->source)
            .primary(y_min ? y_min->source : y_max->source)
            .emit(dh);
          return failure::promise();
        }
      }
    }
    if (limit.inner > 100'000) {
      diagnostic::error("`limit` must be less than 100k")
        .primary(limit)
        .emit(dh);
      return failure::promise();
    }
    if (limit.inner == 0) {
      diagnostic::error("`limit` must be positive").primary(limit).emit(dh);
      return failure::promise();
    }
    return {};
  }

  auto validate_xtype(const located<data>& d, diagnostic_handler& dh) const
    -> failure_or<void> {
    const auto t = type::infer(d.inner);
    if (not t) {
      diagnostic::error("failed to infer type of option").primary(d).emit(dh);
      return failure::promise();
    }
    auto valid = t->kind()
                   .is_any<int64_type, uint64_type, double_type, duration_type,
                           time_type>();
    if (not valid) {
      diagnostic::warning("limit cannot have type `{}`", t->kind())
        .primary(d)
        .emit(dh);
      return failure::promise();
    }
    if (res and not t->kind().is_any<time_type, duration_type>()) {
      diagnostic::warning("cannot group type `{}` with resolution", t->kind())
        .primary(d)
        .primary(res->source)
        .emit(dh);
      return failure::promise();
    }
    return {};
  }

  static auto validate_ytype(const located<data>& d, diagnostic_handler& dh)
    -> failure_or<void> {
    const auto t = type::infer(d.inner);
    if (not t) {
      diagnostic::error("failed to infer type of option").primary(d).emit(dh);
      return failure::promise();
    }
    if (not t->kind()
              .is_any<int64_type, uint64_type, double_type, duration_type>()) {
      diagnostic::error("y-axis cannot have type `{}`", t->kind())
        .primary(d)
        .emit(dh);
      return failure::promise();
    }
    return {};
  }

  auto find_plugins(session ctx) const -> plugins_map {
    auto plugins = plugins_map{};
    auto ident = ast::identifier{"once", location::unknown};
    const auto entity = ast::entity{std::vector{std::move(ident)}};
    for (const auto& [_, call] : y) {
      if (const auto* ptr
          = dynamic_cast<const aggregation_plugin*>(&ctx.reg().get(call))) {
        plugins.emplace_back(ptr, call);
        continue;
      }
      auto wrapped_call = ast::function_call{entity, {call}, call.rpar, false};
      TENZIR_ASSERT(resolve_entities(wrapped_call, ctx));
      const auto* ptr
        = dynamic_cast<const aggregation_plugin*>(&ctx.reg().get(wrapped_call));
      TENZIR_ASSERT(ptr);
      plugins.emplace_back(ptr, std::move(wrapped_call));
    }
    return plugins;
  }

  static auto make_bucket(const plugins_map& plugins, session ctx) -> bucket {
    auto b = bucket{};
    for (const auto& [plugin, arg] : plugins) {
      auto inv = aggregation_plugin::invocation{arg};
      auto instance = plugin->make_aggregation(std::move(inv), ctx);
      TENZIR_ASSERT(instance);
      b.push_back(std::move(instance).unwrap());
    }
    return b;
  }
};

auto handle_y(chart_args& args, ast::expression& y, session ctx, chart_type ty,
              std::string_view operator_name) -> failure_or<void> {
  auto ident = ast::identifier{"once", location::unknown};
  const auto entity = ast::entity{std::vector{std::move(ident)}};
  args.ty = ty;
  args.y_loc = y.get_location();
  return match(
    y,
    [&](ast::record& rec) -> failure_or<void> {
      if (args.ty == chart_type::pie and rec.items.size() != 1) {
        diagnostic::error("`{}` requires exactly one value", operator_name)
          .primary(y)
          .emit(ctx);
        return failure::promise();
      }
      if (rec.items.empty()) {
        diagnostic::error("`{}` requires at least one value", operator_name)
          .primary(y)
          .emit(ctx);
        return failure::promise();
      }
      for (auto& i : rec.items) {
        auto* field = try_as<ast::record::field>(i);
        if (not field) {
          diagnostic::error("cannot use `...` here").primary(y).emit(ctx);
          return failure::promise();
        }
        const auto loc = field->expr.get_location();
        TRY(match(
          field->expr,
          [&](ast::function_call& call) -> failure_or<void> {
            args.y[field->name.name] = std::move(call);
            return {};
          },
          [&](auto& expr) -> failure_or<void> {
            if (args.res) {
              diagnostic::error("an aggregation function is required if "
                                "`resolution` is specified")
                .primary(field->expr)
                .emit(ctx);
              return failure::promise();
            }
            auto result
              = ast::function_call{entity, {std::move(expr)}, loc, false};
            TENZIR_ASSERT(resolve_entities(result, ctx));
            args.y[field->name.name] = std::move(result);
            return {};
          }));
      }
      return {};
    },
    [&](ast::function_call& call) -> failure_or<void> {
      args.y[args.ty == chart_type::pie ? "value" : "y"] = std::move(call);
      return {};
    },
    [&](auto&) -> failure_or<void> {
      if (args.res) {
        diagnostic::error(
          "an aggregation function is required if resolution is specified")
          .primary(y)
          .emit(ctx);
        return failure::promise();
      }
      const auto yname = std::invoke([&]() -> std::string {
        if (auto ss = ast::field_path::try_from(y)) {
          return fmt::format("{}", fmt::join(ss->path()
                                               | std::ranges::views::transform(
                                                 &ast::field_path::segment::id)
                                               | std::ranges::views::transform(
                                                 &ast::identifier::name),
                                             "."));
        }
        if (args.ty == chart_type::pie) {
          return "value";
        }
        return "y";
      });
      const auto loc = y.get_location();
      auto result = ast::function_call{entity, {std::move(y)}, loc, false};
      TENZIR_ASSERT(resolve_entities(result, ctx));
      args.y[yname] = std::move(result);
      return {};
    });
}

auto handle_xlimit(const chart_args& args, ast::binary_op op,
                   located<data> limit, diagnostic_handler& dh)
  -> failure_or<xlimit> {
  const auto& loc = limit.source;
  auto result = match(
    limit.inner,
    [&](const caf::none_t&) -> failure_or<ast::constant> {
      diagnostic::error("limit cannot be `null`").primary(limit).emit(dh);
      return failure::promise();
    },
    [&](const pattern&) -> failure_or<ast::constant> {
      diagnostic::error("limit cannot be a pattern").primary(limit).emit(dh);
      return failure::promise();
    },
    [&](const duration& d) -> failure_or<ast::constant> {
      if (args.res) {
        const auto val = d.count();
        const auto count = std::abs(args.res->inner.count());
        const auto rem = std::abs(val % count);
        if (rem) {
          const auto ceil = val >= 0 ? count - rem : rem;
          const auto floor = val >= 0 ? -rem : rem - count;
          return ast::constant{
            duration{val + (op == ast::binary_op::geq ? floor : ceil)},
            loc,
          };
        }
      }
      return ast::constant{d, loc};
    },
    [&](const time& t) -> failure_or<ast::constant> {
      if (not args.res) {
        return ast::constant{t, loc};
      }
      auto b = time_type::make_arrow_builder(arrow_memory_pool());
      check(append_builder(time_type{}, *b, t));
      auto array = finish(*b);
      auto opts = make_round_temporal_options(args.res->inner);
      auto result
        = op == ast::binary_op::geq
            ? check(arrow::compute::FloorTemporal(array, std::move(opts)))
                .array_as<arrow::TimestampArray>()
            : check(arrow::compute::CeilTemporal(array, std::move(opts)))
                .array_as<arrow::TimestampArray>();
      TENZIR_ASSERT(result->length() == 1);
      return ast::constant{value_at(time_type{}, *result, 0), loc};
    },
    [&](const auto& d) -> failure_or<ast::constant> {
      return ast::constant{d, loc};
    });
  TRY(auto c, result);
  auto expr = ast::binary_expr{args.x.inner(), {op, loc}, c};
  auto&& [legacy, _] = split_legacy_expression(expr);
  return xlimit{
    std::move(limit),
    c.as_data(),
    std::move(legacy),
    std::move(expr),
  };
}

template <chart_type Ty>
class Chart final : public Operator<table_slice, table_slice> {
public:
  explicit Chart(RawArgs<Ty> args) : raw_{std::move(args)} {
  }

  auto process(table_slice input, Push<table_slice>&, OpCtx& ctx)
    -> Task<void> override {
    if (input.rows() == 0 or failed_) {
      co_return;
    }
    if (not ensure_compiled(ctx)) {
      failed_ = true;
      co_return;
    }
    auto slices = filter_input(std::move(input), ctx.dh());
    for (auto&& slice : slices) {
      if (slice.rows() == 0) {
        continue;
      }
      auto sp = session_provider::make(ctx.dh());
      process_slice(slice, sp.as_session());
    }
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    if (failed_ or not args_) {
      co_return FinalizeBehavior::done;
    }
    auto slices = build_output(ctx.dh());
    for (auto&& slice : slices) {
      co_await push(std::move(slice));
    }
    co_return FinalizeBehavior::done;
  }

private:
  auto ensure_compiled(OpCtx& ctx) -> bool {
    if (args_) {
      return true;
    }
    auto sp = session_provider::make(ctx.dh());
    auto s = sp.as_session();
    auto args = chart_args{};
    args.ty = Ty;
    args.x = std::move(raw_.x);
    args.group = std::move(raw_.group);
    args.y_min = std::move(raw_.y_min);
    args.y_max = std::move(raw_.y_max);
    args.res = std::move(raw_.res);
    args.fill = std::move(raw_.fill);
    args.x_log = std::move(raw_.x_log);
    args.y_log = std::move(raw_.y_log);
    args.limit = std::move(raw_.limit);
    args.position = std::move(raw_.position);
    if (auto result = handle_y(args, raw_.y, s, Ty, name()); not result) {
      return false;
    }
    if (raw_.x_min) {
      auto result
        = handle_xlimit(args, ast::binary_op::geq, std::move(*raw_.x_min), s);
      if (not result) {
        return false;
      }
      args.x_min = std::move(*result);
    }
    if (raw_.x_max) {
      auto result
        = handle_xlimit(args, ast::binary_op::leq, std::move(*raw_.x_max), s);
      if (not result) {
        return false;
      }
      args.x_max = std::move(*result);
    }
    if (args.y_min) {
      args.y_min->inner = to_double(std::move(args.y_min->inner));
    }
    if (args.y_max) {
      args.y_max->inner = to_double(std::move(args.y_max->inner));
    }
    if (args.fill) {
      args.fill->inner = to_double(std::move(args.fill->inner));
    }
    if (auto result = args.validate(s.dh()); not result) {
      return false;
    }
    args_ = std::move(args);
    plugins_ = args_->find_plugins(s);
    return true;
  }

  auto name() const -> std::string {
    return fmt::format("chart_{}", to_string(Ty));
  }

  auto process_slice(const table_slice& slice, session ctx) -> void {
    auto& dh = ctx.dh();
    auto xpath = args_->x.path()[0].id.name;
    for (auto i = size_t{1}; i < args_->x.path().size(); ++i) {
      xpath += ".";
      xpath += args_->x.path()[i].id.name;
    }
    auto consumed = size_t{};
    auto xs = eval(args_->x, slice, dh);
    auto gs = get_group_strings(slice, dh);
    TENZIR_ASSERT(gs.type.kind().template is<string_type>());
    if (not xty_) {
      if (not validate_x(xs.type, dh)) {
        consumed += xs.length();
        return;
      }
      xty_ = xs.type;
    }
    if (xs.type != xty_.value()) {
      if (xs.type.kind().template is_not<null_type>()
          or not validate_x(xs.type, dh)) {
        diagnostic::warning("cannot plot different types `{}` and `{}` on "
                            "the x-axis",
                            xty_.value().kind(), xs.type.kind())
          .primary(args_->x)
          .note("skipping invalid events")
          .emit(dh);
        consumed += xs.length();
        return;
      }
    }
    if (args_->res) {
      xs = floor(xs);
    }
    bucket* b = nullptr;
    for (auto i = size_t{};
         auto&& [idx, x] : detail::enumerate<int64_t>(xs.values())) {
      const auto group_name = std::invoke([&]() -> std::string_view {
        if (gs.array->IsNull(idx)) {
          if (args_->group) {
            diagnostic::warning("got group name `null`")
              .primary(args_->group.value())
              .note("using `\"null\"` instead")
              .emit(dh);
            return *gnames_.emplace("null").first;
          }
          return *gnames_.emplace("").first;
        }
        return *gnames_.emplace(value_at(string_type{}, *gs.array, idx)).first;
      });
      auto [newb, new_bucket] = get_bucket(groups_, x, group_name, ctx);
      if (b != newb or new_bucket) {
        if (b) {
          for (auto&& instance : *b) {
            instance->update(subslice(slice, consumed, consumed + i), ctx);
          }
        }
        if (new_bucket) {
          b = &get_groups(groups_, x, ctx)
                 ->emplace(group_name, args_->make_bucket(*plugins_, ctx))
                 .first->second;
        } else {
          b = newb;
        }
        consumed += i;
        i = 0;
      }
      ++i;
    }
    if (b) {
      for (auto&& instance : *b) {
        if (consumed != slice.rows()) {
          instance->update(subslice(slice, consumed, slice.rows()), ctx);
        }
      }
    }
  }

  auto build_output(diagnostic_handler& dh) -> std::vector<table_slice> {
    if (groups_.empty()) {
      diagnostic::warning("chart_{} received no valid data", to_string(Ty))
        .primary(args_->x)
        .emit(dh);
      return {};
    }
    auto xpath = args_->x.path()[0].id.name;
    for (auto i = size_t{1}; i < args_->x.path().size(); ++i) {
      xpath += ".";
      xpath += args_->x.path()[i].id.name;
    }
    auto ynames = detail::stable_map<std::string, bool>{};
    auto b = series_builder{};
    const auto make_yname = [&](std::string_view group, std::string_view y) {
      if (not args_->group) {
        return std::string{y};
      }
      if (args_->y.size() == 1) {
        return std::string{group};
      }
      return fmt::format("{}_{}", group, y);
    };
    const auto add_y = [&](std::string_view group, std::string_view y,
                           bool valid) -> const std::string& {
      auto [it, _] = ynames.try_emplace(make_yname(group, y), valid);
      it->second &= valid;
      return it->first;
    };
    const auto fill_value = args_->fill ? args_->fill->inner : data{};
    const auto fill_at = [&](const data& x) {
      auto r = b.record();
      r.field(xpath).data(x);
      for (const auto& gname : gnames_) {
        for (const auto& [y, _] : args_->y) {
          r.field(make_yname(gname, y)).data(fill_value);
        }
      }
    };
    const auto insert = [&](const data& x, const grouped_bucket& groups) {
      auto r = b.record();
      r.field(xpath).data(x);
      if (args_->fill) {
        for (const auto& gname : gnames_) {
          for (const auto& [y, _] : args_->y) {
            r.field(make_yname(gname, y)).data(fill_value);
          }
        }
      }
      for (const auto& [name, bucket] : groups) {
        TENZIR_ASSERT(args_->y.size() == bucket.size());
        for (const auto& [y, instance] : std::views::zip(args_->y, bucket)) {
          auto value = to_double(instance->get());
          if (args_->fill and is<caf::none_t>(value)) {
            continue;
          }
          auto valid = validate_y(value, make_yname(name, y.first),
                                  y.second.get_location(), dh);
          r.field(add_y(name, y.first, valid)).data(value);
        }
      }
    };
    if (args_->x_min and args_->res) {
      TENZIR_ASSERT(not groups_.empty());
      auto min = std::optional{args_->x_min->rounded};
      const auto& first = groups_.begin()->first;
      if (*min != first) {
        fill_at(*min);
      }
      while (auto gap = find_gap(min, first)) {
        min = gap.value();
        fill_at(std::move(gap).value());
      }
    }
    for (auto prev = std::optional<data>{};
         const auto& [x, gb] : groups_ | std::views::take(args_->limit.inner)) {
      if (args_->res) {
        while (auto gap = find_gap(prev, x)) {
          prev = gap.value();
          fill_at(std::move(gap).value());
        }
      }
      insert(x, gb);
      prev = x;
    }
    if (args_->x_max and args_->res) {
      TENZIR_ASSERT(not groups_.empty());
      auto last = std::optional{groups_.rbegin()->first};
      const auto& max = args_->x_max->rounded;
      while (auto gap = find_gap(last, max)) {
        last = gap.value();
        fill_at(std::move(gap).value());
      }
      if (*last != max) {
        fill_at(max);
      }
    }
    auto slices = b.finish_as_table_slice("tenzir.chart");
    if (slices.size() > 1) {
      diagnostic::warning("got type conflicts, emitting multiple schemas")
        .primary(args_->x)
        .emit(dh);
    }
    const auto limits = detail::stable_map<std::string_view, std::string>{
      {"x_min", args_->x_min ? jsonify_limit(args_->x_min->value.inner) : ""},
      {"x_max", args_->x_max ? jsonify_limit(args_->x_max->value.inner) : ""},
      {"y_min", args_->y_min ? jsonify_limit(args_->y_min->inner) : ""},
      {"y_max", args_->y_max ? jsonify_limit(args_->y_max->inner) : ""},
    };
    auto ynums = std::deque<std::string>{"y"};
    const auto attrs = make_attributes(xpath, ynums, ynames, limits);
    auto result = std::vector<table_slice>{};
    for (auto&& slice : slices) {
      auto schema = type{slice.schema(), std::vector{attrs}};
      result.push_back(cast(std::move(slice), schema));
    }
    return result;
  }

  auto get_group_strings(const table_slice& slice, diagnostic_handler& dh) const
    -> series {
    if (not args_->group) {
      return series::null(string_type{}, detail::narrow<int64_t>(slice.rows()));
    }
    auto b = string_type::make_arrow_builder(arrow_memory_pool());
    auto gss = eval(args_->group.value(), slice, dh);
    for (auto&& gs : gss) {
      if (gs.type.kind().is<null_type>()) {
        check(b->AppendNulls(gs.length()));
        continue;
      }
      if (auto* str = try_as<arrow::StringArray>(*gs.array)) {
        if (gss.parts().size() == 1) {
          return std::move(gs);
        }
        check(append_array(*b, string_type{}, *str));
        continue;
      }
      if (not gs.type.kind()
                .is_any<int64_type, uint64_type, double_type,
                        enumeration_type>()) {
        diagnostic::warning("cannot group type `{}`", gs.type.kind())
          .primary(args_->group.value())
          .emit(dh);
        check(b->AppendNulls(gs.length()));
        continue;
      }
      for (auto&& value : gs.values()) {
        if (is<caf::none_t>(value)) {
          check(b->AppendNull());
          continue;
        }
        const auto f = detail::overload{
          [&](const enumeration& x) {
            return std::string{as<enumeration_type>(gs.type).field(x)};
          },
          [&](const int64_t& x) {
            return fmt::to_string(x);
          },
          [&](const auto&) {
            return fmt::to_string(value);
          },
        };
        check(b->Append(match(value, f)));
      }
    }
    return series{string_type{}, finish(*b)};
  }

  auto get_groups(group_map& map, const data_view& x, session ctx) const
    -> grouped_bucket* {
    const auto xv = materialize(x);
    if (auto it = map.find(xv); it != map.end()) {
      return &it->second;
    }
    if (map.size() == args_->limit.inner) {
      diagnostic::warning("got more than {} data points", args_->limit.inner)
        .primary(args_->x)
        .note("skipping excess data points")
        .hint(
          "consider filtering data or aggregating over a bigger `resolution`")
        .emit(ctx);
      return nullptr;
    }
    return &map[xv];
  }

  auto get_bucket(group_map& map, const data_view& x,
                  const std::string_view group, session ctx) const
    -> std::pair<bucket*, bool> {
    if constexpr (Ty != chart_type::bar and Ty != chart_type::pie) {
      if (is<caf::none_t>(x)) {
        diagnostic::warning("x-axis cannot be `null`")
          .primary(args_->x)
          .emit(ctx);
        return {nullptr, false};
      }
    }
    auto* gs = get_groups(map, x, ctx);
    if (not gs) {
      return {nullptr, false};
    }
    if (auto it = gs->find(group); it != gs->end()) {
      return {&it->second, false};
    }
    return {nullptr, true};
  }

  auto filter_input(table_slice slice, diagnostic_handler& dh)
    -> std::vector<table_slice> {
    // Early exit: no limits set
    if (not args_->x_min and not args_->x_max) {
      return {std::move(slice)};
    }
    // Build combined expression from x_min and x_max
    const auto expr = std::invoke([&]() -> ast::expression {
      if (args_->x_min and args_->x_max) {
        return ast::binary_expr{
          args_->x_min->expr,
          {ast::binary_op::and_, location::unknown},
          args_->x_max->expr,
        };
      }
      if (args_->x_min) {
        return args_->x_min->expr;
      }
      TENZIR_ASSERT(args_->x_max);
      return args_->x_max->expr;
    });
    // Evaluate expression and extract filtered slices
    auto results = std::vector<table_slice>{};
    auto offset = int64_t{0};
    for (auto& filter : eval(expr, slice, dh)) {
      const auto* array = try_as<arrow::BooleanArray>(&*filter.array);
      TENZIR_ASSERT(array);
      const auto len = array->length();
      // Use shared helper to extract contiguous runs
      auto subslices = split_contiguous_true_runs(*array, slice, offset);
      results.insert(results.end(), std::make_move_iterator(subslices.begin()),
                     std::make_move_iterator(subslices.end()));
      offset += len;
    }
    return results;
  }

  auto find_gap(std::optional<data>& prev, const data& curr) const
    -> std::optional<data> {
    if (not prev) {
      prev = curr;
      return std::nullopt;
    }
    if (is<caf::none_t>(*prev) or is<caf::none_t>(curr)) {
      return std::nullopt;
    }
    auto result = match(
      std::tie(curr, *prev),
      [&](const duration& c, const duration& p) -> std::optional<data> {
        if (c - p > args_->res->inner) {
          return p + args_->res->inner;
        }
        return std::nullopt;
      },
      [&](const time& c, const time& p) -> std::optional<data> {
        if (c - p > args_->res->inner) {
          return p + args_->res->inner;
        }
        return std::nullopt;
      },
      [](const auto&, const auto&) -> std::optional<data> {
        TENZIR_UNREACHABLE();
      });
    return result;
  }

  auto make_attributes(
    const std::string& xpath, std::deque<std::string>& ynums,
    detail::stable_map<std::string, bool>& ynames,
    const detail::stable_map<std::string_view, std::string>& limits) const
    -> std::vector<type::attribute_view> {
    auto attrs = std::vector<type::attribute_view>{
      {"chart", to_string(Ty)},
      {"position", args_->position.inner},
      {"x_axis_type", args_->x_log ? "log" : "linear"},
      {"y_axis_type", args_->y_log ? "log" : "linear"},
      {"x", xpath},
    };
    for (const auto& [name, value] : limits) {
      if (not value.empty()) {
        attrs.emplace_back(name, value);
      }
    }
    for (auto i = ynums.size(); i < args_->y.size() or i < ynames.size(); ++i) {
      ynums.emplace_back(fmt::format("y{}", i));
    }
    auto names = std::views::filter(ynames, [](auto&& x) {
      return x.second;
    });
    for (const auto& [num, field] : std::views::zip(ynums, names)) {
      attrs.emplace_back(num, field.first);
    }
    return attrs;
  }

  auto validate_x(const type& ty, diagnostic_handler& dh) const -> bool {
    auto valid = ty.kind()
                   .is_any<int64_type, uint64_type, double_type, duration_type,
                           time_type>();
    if constexpr (Ty == chart_type::bar or Ty == chart_type::pie) {
      valid |= ty.kind().is_any<null_type, ip_type, subnet_type, string_type>();
    }
    if (not valid) {
      diagnostic::warning("x-axis cannot have type `{}`", ty.kind())
        .note("skipping invalid events")
        .primary(args_->x)
        .emit(dh);
      return false;
    }
    if (args_->res and not ty.kind().is_any<time_type, duration_type>()) {
      diagnostic::warning("cannot group type `{}` with resolution", ty.kind())
        .note("skipping invalid events")
        .primary(args_->x)
        .primary(args_->res->source)
        .emit(dh);
      return false;
    }
    return true;
  }

  auto validate_y(const data& d, std::string_view yname, tenzir::location loc,
                  diagnostic_handler& dh) const -> bool {
    const auto ty = type::infer(d);
    if (not ty) {
      diagnostic::warning("failed to infer type of `y`")
        .primary(loc)
        .note(fmt::format("skipping {}", yname))
        .emit(dh);
      return false;
    }
    if (not ty->kind()
              .is_any<null_type, int64_type, uint64_type, double_type,
                      duration_type>()) {
      diagnostic::warning("y-axis cannot have type `{}`", ty->kind())
        .primary(loc)
        .note(fmt::format("skipping {}", yname))
        .emit(dh);
      return false;
    }
    if (args_->y_min or args_->y_max) {
      const auto lty = args_->y_min ? type::infer(args_->y_min->inner)
                                    : type::infer(args_->y_max->inner);
      if (not lty) {
        diagnostic::warning("failed to infer type of limit")
          .primary(args_->y_min ? args_->y_min->source : args_->y_max->source)
          .note(fmt::format("skipping {}", yname))
          .emit(dh);
        return false;
      }
      if (lty->kind() != ty->kind()) {
        diagnostic::warning("limit has a different type `{}` from `y` type "
                            "`{}`",
                            lty->kind(), ty->kind())
          .primary(args_->y_min ? args_->y_min->source : args_->y_max->source)
          .note(fmt::format("skipping {}", yname))
          .emit(dh);
        return false;
      }
    }
    return true;
  }

  auto floor(const series& xs) const -> series {
    return match(
      *xs.array,
      [&](const arrow::DurationArray& array) -> series {
        auto b = duration_type::make_arrow_builder(arrow_memory_pool());
        check(b->Reserve(array.length()));
        for (auto i = int64_t{0}; i < array.length(); i++) {
          if (array.IsNull(i)) {
            check(b->AppendNull());
            continue;
          }
          const auto val = array.Value(i);
          const auto count = std::abs(args_->res->inner.count());
          const auto rem = std::abs(val % count);
          if (rem == 0) {
            check(b->Append(val));
            continue;
          }
          const auto floor = val >= 0 ? -rem : rem - count;
          check(b->Append(val + floor));
        }
        return {duration_type{}, finish(*b)};
      },
      [&](const arrow::TimestampArray& array) -> series {
        auto opts = make_round_temporal_options(args_->res->inner);
        return {time_type{},
                check(arrow::compute::FloorTemporal(array, std::move(opts)))
                  .array_as<arrow::TimestampArray>()};
      },
      [&](const auto&) -> series {
        TENZIR_UNREACHABLE();
      });
  }

  RawArgs<Ty> raw_;
  std::optional<chart_args> args_;
  std::optional<plugins_map> plugins_;
  std::optional<type> xty_;
  group_map groups_;
  std::unordered_set<std::string> gnames_;
  bool failed_ = false;
};

} // namespace

class PluginArea : public virtual OperatorPlugin {
  auto name() const -> std::string override {
    return fmt::format("tql2.chart_area");
  }

  auto describe() const -> Description override {
    auto d
      = tenzir::Describer<RawArgs<chart_type::area>, Chart<chart_type::area>>{};
    d.named("x", &RawArgs<chart_type::area>::x);
    d.named("y", &RawArgs<chart_type::area>::y, "any");
    d.named("x_min", &RawArgs<chart_type::area>::x_min);
    d.named("x_max", &RawArgs<chart_type::area>::x_max);
    d.named("y_min", &RawArgs<chart_type::area>::y_min);
    d.named("y_max", &RawArgs<chart_type::area>::y_max);

    d.named("resolution", &RawArgs<chart_type::area>::res);
    d.named("fill", &RawArgs<chart_type::area>::fill);

    d.named("x_log", &RawArgs<chart_type::area>::x_log);
    d.named("y_log", &RawArgs<chart_type::area>::y_log);

    d.named("group", &RawArgs<chart_type::area>::group, "any");
    d.named_optional("position", &RawArgs<chart_type::area>::position);
    d.named_optional("_limit", &RawArgs<chart_type::area>::limit);
    return d.without_optimize();
  }
};

class PluginBar : public virtual OperatorPlugin {
  auto name() const -> std::string override {
    return fmt::format("tql2.chart_bar");
  }

  auto describe() const -> Description override {
    auto d
      = tenzir::Describer<RawArgs<chart_type::bar>, Chart<chart_type::bar>>{};

    d.named({"x", "label"}, &RawArgs<chart_type::bar>::x);
    d.named({"y", "value"}, &RawArgs<chart_type::bar>::y, "any");

    d.named("x_min", &RawArgs<chart_type::bar>::x_min);
    d.named("x_max", &RawArgs<chart_type::bar>::x_max);
    d.named("y_min", &RawArgs<chart_type::bar>::y_min);
    d.named("y_max", &RawArgs<chart_type::bar>::y_max);

    d.named("resolution", &RawArgs<chart_type::bar>::res);
    d.named("fill", &RawArgs<chart_type::bar>::fill);

    d.named("x_log", &RawArgs<chart_type::bar>::x_log);
    d.named("y_log", &RawArgs<chart_type::bar>::y_log);

    d.named("group", &RawArgs<chart_type::bar>::group, "any");
    d.named_optional("position", &RawArgs<chart_type::bar>::position);
    d.named_optional("_limit", &RawArgs<chart_type::bar>::limit);
    return d.without_optimize();
  }
};

class PluginLine : public virtual OperatorPlugin {
  auto name() const -> std::string override {
    return fmt::format("tql2.chart_line");
  }

  auto describe() const -> Description override {
    auto d
      = tenzir::Describer<RawArgs<chart_type::line>, Chart<chart_type::line>>{};
    d.named("x", &RawArgs<chart_type::line>::x);
    d.named("y", &RawArgs<chart_type::line>::y, "any");

    d.named("x_min", &RawArgs<chart_type::line>::x_min);
    d.named("x_max", &RawArgs<chart_type::line>::x_max);
    d.named("y_min", &RawArgs<chart_type::line>::y_min);
    d.named("y_max", &RawArgs<chart_type::line>::y_max);

    d.named("resolution", &RawArgs<chart_type::line>::res);
    d.named("fill", &RawArgs<chart_type::line>::fill);

    d.named("x_log", &RawArgs<chart_type::line>::x_log);
    d.named("y_log", &RawArgs<chart_type::line>::y_log);

    d.named("group", &RawArgs<chart_type::line>::group, "any");
    d.named_optional("_limit", &RawArgs<chart_type::line>::limit);
    return d.without_optimize();
  }
};

class PluginPie : public virtual OperatorPlugin {
  auto name() const -> std::string override {
    return fmt::format("tql2.chart_pie");
  }

  auto describe() const -> Description override {
    auto d
      = tenzir::Describer<RawArgs<chart_type::pie>, Chart<chart_type::pie>>{};

    d.named({"x", "label"}, &RawArgs<chart_type::pie>::x);
    d.named({"y", "value"}, &RawArgs<chart_type::pie>::y, "any");

    d.named("group", &RawArgs<chart_type::pie>::group, "any");
    d.named_optional("_limit", &RawArgs<chart_type::pie>::limit);
    return d.without_optimize();
  }
};

} // namespace tenzir::plugins::chart

TENZIR_REGISTER_PLUGIN(tenzir::plugins::chart::PluginArea)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::chart::PluginBar)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::chart::PluginLine)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::chart::PluginPie)
