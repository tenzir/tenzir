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
#include "tenzir/detail/stable_set.hpp"
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

auto jsonify_limit(data const& d) -> std::string {
  auto result = std::string{};
  auto const printer = tenzir::json_printer{tenzir::json_printer_options{
    .tql = true,
    .numeric_durations = true,
  }};
  auto it = std::back_inserter(result);
  auto success = printer.print(it, make_view_wrapper(d));
  TENZIR_ASSERT(success);
  return result;
}

using call_map = detail::stable_map<std::string, ast::function_call>;
using plugins_map
  = std::vector<std::pair<aggregation_plugin const*, ast::function_call>>;

struct Bucket {
  std::vector<std::unique_ptr<aggregation_instance>> instances;
};

struct GroupedBucket {
  detail::stable_map<std::string, Bucket> groups;
};

struct xlimit {
  located<data> value;
  data rounded;
  expression legacy_expr;
  ast::expression expr;
};

// Raw arguments struct used for parsing
template <chart_type Ty>
struct ChartArgs {
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

auto find_plugins(call_map const& y, session ctx) -> plugins_map {
  auto result = plugins_map{};
  auto ident = ast::identifier{"once", location::unknown};
  auto const entity = ast::entity{std::vector{std::move(ident)}};
  for (auto const& [_, call] : y) {
    if (auto const* ptr
        = dynamic_cast<aggregation_plugin const*>(&ctx.reg().get(call))) {
      result.emplace_back(ptr, call);
      continue;
    }
    auto wrapped_call = ast::function_call{entity, {call}, call.rpar, false};
    TENZIR_ASSERT(resolve_entities(wrapped_call, ctx));
    auto const* ptr
      = dynamic_cast<aggregation_plugin const*>(&ctx.reg().get(wrapped_call));
    TENZIR_ASSERT(ptr);
    result.emplace_back(ptr, std::move(wrapped_call));
  }
  return result;
}

auto make_bucket(plugins_map const& plugins, session ctx) -> Bucket {
  auto b = Bucket{};
  for (auto const& [plugin, arg] : plugins) {
    auto inv = aggregation_plugin::invocation{arg};
    auto instance = plugin->make_aggregation(std::move(inv), ctx);
    TENZIR_ASSERT(instance);
    b.instances.push_back(std::move(instance).unwrap());
  }
  return b;
}

template <chart_type Ty>
auto handle_y(call_map& y_out, location& y_loc_out,
              std::optional<located<duration>> const& res, ast::expression& y,
              session ctx, std::string_view operator_name) -> failure_or<void> {
  auto ident = ast::identifier{"once", location::unknown};
  auto const entity = ast::entity{std::vector{std::move(ident)}};
  y_loc_out = y.get_location();
  return match(
    y,
    [&](ast::record& rec) -> failure_or<void> {
      if (Ty == chart_type::pie and rec.items.size() != 1) {
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
        auto const loc = field->expr.get_location();
        TRY(match(
          field->expr,
          [&](ast::function_call& call) -> failure_or<void> {
            y_out[field->name.name] = std::move(call);
            return {};
          },
          [&](auto& expr) -> failure_or<void> {
            if (res) {
              diagnostic::error("an aggregation function is required if "
                                "`resolution` is specified")
                .primary(field->expr)
                .emit(ctx);
              return failure::promise();
            }
            auto result
              = ast::function_call{entity, {std::move(expr)}, loc, false};
            TENZIR_ASSERT(resolve_entities(result, ctx));
            y_out[field->name.name] = std::move(result);
            return {};
          }));
      }
      return {};
    },
    [&](ast::function_call& call) -> failure_or<void> {
      y_out[Ty == chart_type::pie ? "value" : "y"] = std::move(call);
      return {};
    },
    [&](auto&) -> failure_or<void> {
      if (res) {
        diagnostic::error(
          "an aggregation function is required if resolution is specified")
          .primary(y)
          .emit(ctx);
        return failure::promise();
      }
      auto const yname = std::invoke([&]() -> std::string {
        if (auto ss = ast::field_path::try_from(y)) {
          return fmt::format("{}", fmt::join(ss->path()
                                               | std::ranges::views::transform(
                                                 &ast::field_path::segment::id)
                                               | std::ranges::views::transform(
                                                 &ast::identifier::name),
                                             "."));
        }
        if (Ty == chart_type::pie) {
          return "value";
        }
        return "y";
      });
      auto const loc = y.get_location();
      auto result = ast::function_call{entity, {std::move(y)}, loc, false};
      TENZIR_ASSERT(resolve_entities(result, ctx));
      y_out[yname] = std::move(result);
      return {};
    });
}

template <chart_type Ty>
auto handle_xlimit(ChartArgs<Ty> const& args, ast::binary_op op,
                   located<data> limit, diagnostic_handler& dh)
  -> failure_or<xlimit> {
  auto const& loc = limit.source;
  auto result = match(
    limit.inner,
    [&](const caf::none_t&) -> failure_or<ast::constant> {
      diagnostic::error("limit cannot be `null`").primary(limit).emit(dh);
      return failure::promise();
    },
    [&](pattern const&) -> failure_or<ast::constant> {
      diagnostic::error("limit cannot be a pattern").primary(limit).emit(dh);
      return failure::promise();
    },
    [&](duration const& d) -> failure_or<ast::constant> {
      if (args.res) {
        auto const val = d.count();
        auto const count = std::abs(args.res->inner.count());
        auto const rem = std::abs(val % count);
        if (rem) {
          auto const ceil = val >= 0 ? count - rem : rem;
          auto const floor = val >= 0 ? -rem : rem - count;
          return ast::constant{
            duration{val + (op == ast::binary_op::geq ? floor : ceil)},
            loc,
          };
        }
      }
      return ast::constant{d, loc};
    },
    [&](time const& t) -> failure_or<ast::constant> {
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
                .template array_as<arrow::TimestampArray>()
            : check(arrow::compute::CeilTemporal(array, std::move(opts)))
                .template array_as<arrow::TimestampArray>();
      TENZIR_ASSERT(result->length() == 1);
      return ast::constant{value_at(time_type{}, *result, 0), loc};
    },
    [&](auto const& d) -> failure_or<ast::constant> {
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

struct Prepared {
  call_map y;
  plugins_map plugins;
  std::optional<xlimit> x_min;
  std::optional<xlimit> x_max;
  location y_loc;
};

template <chart_type Ty>
class Chart final : public Operator<table_slice, table_slice> {
public:
  explicit Chart(ChartArgs<Ty> args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    prep_ = prepare(ctx);
    co_return;
  }

  auto process(table_slice input, Push<table_slice>&, OpCtx& ctx)
    -> Task<void> override {
    if (input.rows() == 0 or not prep_) {
      co_return;
    }
    auto slice = filter_input(std::move(input), ctx.dh());
    if (slice.rows() > 0) {
      auto sp = session_provider::make(ctx.dh());
      process_slice(slice, sp.as_session());
    }
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    if (not prep_) {
      co_return FinalizeBehavior::done;
    }
    auto slices = build_output(ctx.dh());
    for (auto&& slice : slices) {
      co_await push(std::move(slice));
    }
    co_return FinalizeBehavior::done;
  }

private:
  auto prepare(OpCtx& ctx) -> std::optional<Prepared> {
    auto sp = session_provider::make(ctx.dh());
    auto s = sp.as_session();
    auto prep = Prepared{};

    auto y_copy = args_.y;
    if (auto result
        = handle_y<Ty>(prep.y, prep.y_loc, args_.res, y_copy, s, name());
        not result) {
      return std::nullopt;
    }

    if (args_.x_min) {
      auto result = handle_xlimit(args_, ast::binary_op::geq, *args_.x_min, s);
      if (not result) {
        return std::nullopt;
      }
      prep.x_min = std::move(*result);
    }
    if (args_.x_max) {
      auto result = handle_xlimit(args_, ast::binary_op::leq, *args_.x_max, s);
      if (not result) {
        return std::nullopt;
      }
      prep.x_max = std::move(*result);
    }

    // Convert y_min, y_max, fill to double in args
    if (args_.y_min) {
      args_.y_min->inner = to_double(std::move(args_.y_min->inner));
    }
    if (args_.y_max) {
      args_.y_max->inner = to_double(std::move(args_.y_max->inner));
    }
    if (args_.fill) {
      args_.fill->inner = to_double(std::move(args_.fill->inner));
    }

    prep.plugins = find_plugins(prep.y, s);
    return prep;
  }

  auto name() const -> std::string {
    return fmt::format("chart_{}", to_string(Ty));
  }

  auto process_slice(table_slice const& slice, session ctx) -> void {
    auto& dh = ctx.dh();
    auto xpath = args_.x.path()[0].id.name;
    for (auto i = size_t{1}; i < args_.x.path().size(); ++i) {
      xpath += ".";
      xpath += args_.x.path()[i].id.name;
    }
    auto consumed = size_t{};
    auto xs = eval(args_.x, slice, dh);
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
          .primary(args_.x)
          .note("skipping invalid events")
          .emit(dh);
        consumed += xs.length();
        return;
      }
    }
    if (args_.res) {
      xs = floor(xs);
    }
    Bucket* b = nullptr;
    for (auto i = size_t{};
         auto&& [idx, x] : detail::enumerate<int64_t>(xs.values())) {
      auto const group_name = std::invoke([&]() -> std::string {
        if (gs.array->IsNull(idx)) {
          if (args_.group) {
            diagnostic::warning("got group name `null`")
              .primary(args_.group.value())
              .note("using `\"null\"` instead")
              .emit(dh);
            return std::string{"null"};
          }
          return std::string{};
        }
        return std::string{value_at(string_type{}, *gs.array, idx)};
      });
      auto [newb, new_bucket] = get_bucket(groups_, x, group_name, ctx);
      if (b != newb or new_bucket) {
        if (b) {
          for (auto&& instance : b->instances) {
            instance->update(subslice(slice, consumed, consumed + i), ctx);
          }
        }
        if (new_bucket) {
          b = &get_groups(groups_, x, ctx)
                 ->groups.emplace(group_name, make_bucket(prep_->plugins, ctx))
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
      for (auto&& instance : b->instances) {
        if (consumed != slice.rows()) {
          instance->update(subslice(slice, consumed, slice.rows()), ctx);
        }
      }
    }
  }

  auto build_output(diagnostic_handler& dh) -> std::vector<table_slice> {
    if (groups_.empty()) {
      diagnostic::warning("chart_{} received no valid data", to_string(Ty))
        .primary(args_.x)
        .emit(dh);
      return {};
    }
    auto xpath = args_.x.path()[0].id.name;
    for (auto i = size_t{1}; i < args_.x.path().size(); ++i) {
      xpath += ".";
      xpath += args_.x.path()[i].id.name;
    }
    // Collect unique group names in insertion order
    auto all_group_names = detail::stable_set<std::string>{};
    for (auto const& [_, gb] : groups_) {
      for (auto const& [gname, _] : gb.groups) {
        all_group_names.emplace(gname);
      }
    }
    auto ynames = detail::stable_map<std::string, bool>{};
    auto b = series_builder{};
    auto const make_yname = [&](std::string_view group, std::string_view y) {
      if (not args_.group) {
        return std::string{y};
      }
      if (prep_->y.size() == 1) {
        return std::string{group};
      }
      return fmt::format("{}_{}", group, y);
    };
    auto const add_y = [&](std::string_view group, std::string_view y,
                           bool valid) -> std::string const& {
      auto [it, _] = ynames.try_emplace(make_yname(group, y), valid);
      it->second &= valid;
      return it->first;
    };
    auto const fill_value = args_.fill ? args_.fill->inner : data{};
    auto const fill_at = [&](data const& x) {
      auto r = b.record();
      r.field(xpath).data(x);
      for (auto const& gname : all_group_names) {
        for (auto const& [y, _] : prep_->y) {
          r.field(make_yname(gname, y)).data(fill_value);
        }
      }
    };
    auto const insert = [&](data const& x, GroupedBucket const& groups) {
      auto r = b.record();
      r.field(xpath).data(x);
      if (args_.fill) {
        for (auto const& gname : all_group_names) {
          for (auto const& [y, _] : prep_->y) {
            r.field(make_yname(gname, y)).data(fill_value);
          }
        }
      }
      for (auto const& [name, bucket] : groups.groups) {
        TENZIR_ASSERT(prep_->y.size() == bucket.instances.size());
        for (auto const& [y, instance] :
             std::views::zip(prep_->y, bucket.instances)) {
          auto value = to_double(instance->get());
          if (args_.fill and is<caf::none_t>(value)) {
            continue;
          }
          auto valid = validate_y(value, make_yname(name, y.first),
                                  y.second.get_location(), dh);
          r.field(add_y(name, y.first, valid)).data(value);
        }
      }
    };
    if (prep_->x_min and args_.res) {
      TENZIR_ASSERT(not groups_.empty());
      auto min = std::optional{prep_->x_min->rounded};
      auto const& first = groups_.begin()->first;
      if (*min != first) {
        fill_at(*min);
      }
      while (auto gap = find_gap(min, first)) {
        min = gap.value();
        fill_at(std::move(gap).value());
      }
    }
    for (auto prev = std::optional<data>{};
         auto const& [x, gb] : groups_ | std::views::take(args_.limit.inner)) {
      if (args_.res) {
        while (auto gap = find_gap(prev, x)) {
          prev = gap.value();
          fill_at(std::move(gap).value());
        }
      }
      insert(x, gb);
      prev = x;
    }
    if (prep_->x_max and args_.res) {
      TENZIR_ASSERT(not groups_.empty());
      auto last = std::optional{groups_.rbegin()->first};
      auto const& max = prep_->x_max->rounded;
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
        .primary(args_.x)
        .emit(dh);
    }
    auto const limits = detail::stable_map<std::string_view, std::string>{
      {"x_min", prep_->x_min ? jsonify_limit(prep_->x_min->value.inner) : ""},
      {"x_max", prep_->x_max ? jsonify_limit(prep_->x_max->value.inner) : ""},
      {"y_min", args_.y_min ? jsonify_limit(args_.y_min->inner) : ""},
      {"y_max", args_.y_max ? jsonify_limit(args_.y_max->inner) : ""},
    };
    auto ynums = std::deque<std::string>{"y"};
    auto const attrs = make_attributes(xpath, ynums, ynames, limits);
    auto result = std::vector<table_slice>{};
    for (auto&& slice : slices) {
      auto schema = type{slice.schema(), std::vector{attrs}};
      result.push_back(cast(std::move(slice), schema));
    }
    return result;
  }

  auto get_group_strings(table_slice const& slice, diagnostic_handler& dh) const
    -> series {
    if (not args_.group) {
      return series::null(string_type{}, detail::narrow<int64_t>(slice.rows()));
    }
    auto b = string_type::make_arrow_builder(arrow_memory_pool());
    auto gss = eval(args_.group.value(), slice, dh);
    for (auto&& gs : gss) {
      if (gs.type.kind().template is<null_type>()) {
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
                .template is_any<int64_type, uint64_type, double_type,
                                 enumeration_type>()) {
        diagnostic::warning("cannot group type `{}`", gs.type.kind())
          .primary(args_.group.value())
          .emit(dh);
        check(b->AppendNulls(gs.length()));
        continue;
      }
      for (auto&& value : gs.values()) {
        if (is<caf::none_t>(value)) {
          check(b->AppendNull());
          continue;
        }
        auto const f = detail::overload{
          [&](enumeration const& x) {
            return std::string{as<enumeration_type>(gs.type).field(x)};
          },
          [&](int64_t const& x) {
            return fmt::to_string(x);
          },
          [&](auto const&) {
            return fmt::to_string(value);
          },
        };
        check(b->Append(match(value, f)));
      }
    }
    return series{string_type{}, finish(*b)};
  }

  auto get_groups(std::map<data, GroupedBucket>& map, data_view const& x,
                  session ctx) const -> GroupedBucket* {
    auto const xv = materialize(x);
    if (auto it = map.find(xv); it != map.end()) {
      return &it->second;
    }
    if (map.size() == args_.limit.inner) {
      diagnostic::warning("got more than {} data points", args_.limit.inner)
        .primary(args_.x)
        .note("skipping excess data points")
        .hint(
          "consider filtering data or aggregating over a bigger `resolution`")
        .emit(ctx);
      return nullptr;
    }
    return &map[xv];
  }

  auto get_bucket(std::map<data, GroupedBucket>& map, data_view const& x,
                  std::string const& group, session ctx) const
    -> std::pair<Bucket*, bool> {
    if constexpr (Ty != chart_type::bar and Ty != chart_type::pie) {
      if (is<caf::none_t>(x)) {
        diagnostic::warning("x-axis cannot be `null`")
          .primary(args_.x)
          .emit(ctx);
        return {nullptr, false};
      }
    }
    auto* gs = get_groups(map, x, ctx);
    if (not gs) {
      return {nullptr, false};
    }
    if (auto it = gs->groups.find(group); it != gs->groups.end()) {
      return {&it->second, false};
    }
    return {nullptr, true};
  }

  auto filter_input(table_slice slice, diagnostic_handler& dh) -> table_slice {
    // Early exit: no limits set
    if (not prep_->x_min and not prep_->x_max) {
      return slice;
    }
    // Build combined expression from x_min and x_max
    auto const expr = std::invoke([&]() -> ast::expression {
      if (prep_->x_min and prep_->x_max) {
        return ast::binary_expr{
          prep_->x_min->expr,
          {ast::binary_op::and_, location::unknown},
          prep_->x_max->expr,
        };
      }
      if (prep_->x_min) {
        return prep_->x_min->expr;
      }
      TENZIR_ASSERT(prep_->x_max);
      return prep_->x_max->expr;
    });
    // Evaluate expression and collect filtered slices
    auto results = std::vector<table_slice>{};
    auto offset = int64_t{0};
    for (auto& f : eval(expr, slice, dh)) {
      auto const* array = try_as<arrow::BooleanArray>(&*f.array);
      TENZIR_ASSERT(array);
      auto const len = array->length();
      results.push_back(
        tenzir::filter(subslice(slice, offset, offset + len), *array));
      offset += len;
    }
    return concatenate(std::move(results));
  }

  auto find_gap(std::optional<data>& prev, data const& curr) const
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
      [&](duration const& c, duration const& p) -> std::optional<data> {
        if (c - p > args_.res->inner) {
          return p + args_.res->inner;
        }
        return std::nullopt;
      },
      [&](time const& c, time const& p) -> std::optional<data> {
        if (c - p > args_.res->inner) {
          return p + args_.res->inner;
        }
        return std::nullopt;
      },
      [](auto const&, auto const&) -> std::optional<data> {
        TENZIR_UNREACHABLE();
      });
    return result;
  }

  auto make_attributes(
    std::string const& xpath, std::deque<std::string>& ynums,
    detail::stable_map<std::string, bool>& ynames,
    detail::stable_map<std::string_view, std::string> const& limits) const
    -> std::vector<type::attribute_view> {
    auto attrs = std::vector<type::attribute_view>{
      {"chart", to_string(Ty)},
      {"position", args_.position.inner},
      {"x_axis_type", args_.x_log ? "log" : "linear"},
      {"y_axis_type", args_.y_log ? "log" : "linear"},
      {"x", xpath},
    };
    for (auto const& [name, value] : limits) {
      if (not value.empty()) {
        attrs.emplace_back(name, value);
      }
    }
    for (auto i = ynums.size(); i < prep_->y.size() or i < ynames.size(); ++i) {
      ynums.emplace_back(fmt::format("y{}", i));
    }
    auto names = std::views::filter(ynames, [](auto&& x) {
      return x.second;
    });
    for (auto const& [num, field] : std::views::zip(ynums, names)) {
      attrs.emplace_back(num, field.first);
    }
    return attrs;
  }

  auto validate_x(type const& ty, diagnostic_handler& dh) const -> bool {
    auto valid = ty.kind()
                   .is_any<int64_type, uint64_type, double_type, duration_type,
                           time_type>();
    if constexpr (Ty == chart_type::bar or Ty == chart_type::pie) {
      valid |= ty.kind().is_any<null_type, ip_type, subnet_type, string_type>();
    }
    if (not valid) {
      diagnostic::warning("x-axis cannot have type `{}`", ty.kind())
        .note("skipping invalid events")
        .primary(args_.x)
        .emit(dh);
      return false;
    }
    if (args_.res and not ty.kind().is_any<time_type, duration_type>()) {
      diagnostic::warning("cannot group type `{}` with resolution", ty.kind())
        .note("skipping invalid events")
        .primary(args_.x)
        .primary(args_.res->source)
        .emit(dh);
      return false;
    }
    return true;
  }

  auto validate_y(data const& d, std::string_view yname, tenzir::location loc,
                  diagnostic_handler& dh) const -> bool {
    auto const ty = type::infer(d);
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
    if (args_.y_min or args_.y_max) {
      auto const lty = args_.y_min ? type::infer(args_.y_min->inner)
                                   : type::infer(args_.y_max->inner);
      if (not lty) {
        diagnostic::warning("failed to infer type of limit")
          .primary(args_.y_min ? args_.y_min->source : args_.y_max->source)
          .note(fmt::format("skipping {}", yname))
          .emit(dh);
        return false;
      }
      if (lty->kind() != ty->kind()) {
        diagnostic::warning("limit has a different type `{}` from `y` type "
                            "`{}`",
                            lty->kind(), ty->kind())
          .primary(args_.y_min ? args_.y_min->source : args_.y_max->source)
          .note(fmt::format("skipping {}", yname))
          .emit(dh);
        return false;
      }
    }
    return true;
  }

  auto floor(series const& xs) const -> series {
    return match(
      *xs.array,
      [&](arrow::DurationArray const& array) -> series {
        auto b = duration_type::make_arrow_builder(arrow_memory_pool());
        check(b->Reserve(array.length()));
        for (auto i = int64_t{0}; i < array.length(); i++) {
          if (array.IsNull(i)) {
            check(b->AppendNull());
            continue;
          }
          const auto val = array.Value(i);
          const auto count = std::abs(args_.res->inner.count());
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
      [&](arrow::TimestampArray const& array) -> series {
        auto opts = make_round_temporal_options(args_.res->inner);
        return {time_type{},
                check(arrow::compute::FloorTemporal(array, std::move(opts)))
                  .template array_as<arrow::TimestampArray>()};
      },
      [&](auto const&) -> series {
        TENZIR_UNREACHABLE();
      });
  }

  // input
  ChartArgs<Ty> args_;
  // cache
  std::optional<Prepared> prep_;
  // state
  std::optional<type> xty_;
  std::map<data, GroupedBucket> groups_;
};

// Helper validation for x limit types
auto validate_x_limit_type(located<data> const& d,
                           std::optional<located<duration>> const& res,
                           DescribeCtx& ctx) -> void {
  auto const t = type::infer(d.inner);
  if (not t) {
    diagnostic::error("failed to infer type of option").primary(d).emit(ctx);
    return;
  }
  auto valid = t->kind()
                 .is_any<int64_type, uint64_type, double_type, duration_type,
                         time_type>();
  if (not valid) {
    diagnostic::error("limit cannot have type `{}`", t->kind())
      .primary(d)
      .emit(ctx);
    return;
  }
  if (res && ! t->kind().is_any<time_type, duration_type>()) {
    diagnostic::error("cannot group type `{}` with resolution", t->kind())
      .primary(d)
      .primary(res->source)
      .emit(ctx);
    return;
  }
}

// Helper validation for y limit types
auto validate_y_limit_type(located<data> const& d, DescribeCtx& ctx) -> void {
  auto const t = type::infer(d.inner);
  if (not t) {
    diagnostic::error("failed to infer type of option").primary(d).emit(ctx);
    return;
  }
  if (! t->kind().is_any<int64_type, uint64_type, double_type, duration_type>()) {
    diagnostic::error("y-axis cannot have type `{}`", t->kind())
      .primary(d)
      .emit(ctx);
    return;
  }
}

// Validation for pie chart (only has limit)
auto validate_chart_pie(auto limit, DescribeCtx& ctx) -> Empty {
  if (auto lim = ctx.get(limit)) {
    if (lim->inner == 0) {
      diagnostic::error("`limit` must be positive").primary(*lim).emit(ctx);
    }
    if (lim->inner > 100'000) {
      diagnostic::error("`limit` must be less than 100k")
        .primary(*lim)
        .emit(ctx);
    }
  }
  return {};
}

// Unified validation function for area, bar, line charts
auto validate_chart_common(auto limit, auto position, auto x_min, auto x_max,
                           auto y_min, auto y_max, auto res, auto fill,
                           DescribeCtx& ctx) -> Empty {
  // Validate limit (common to all chart types)
  if (auto lim = ctx.get(limit)) {
    if (lim->inner == 0) {
      diagnostic::error("`limit` must be positive").primary(*lim).emit(ctx);
    }
    if (lim->inner > 100'000) {
      diagnostic::error("`limit` must be less than 100k")
        .primary(*lim)
        .emit(ctx);
    }
  }

  // Validate position (only for area, bar - not line or pie)
  if (auto pos = ctx.get(position)) {
    // Check if this is actually a string type (not just any optional)
    if constexpr (requires {
                    pos->inner;
                    typename decltype(pos->inner)::value_type;
                  }) {
      // It's a string
      if (pos->inner != "stacked" && pos->inner != "grouped") {
        diagnostic::error("unsupported `position`")
          .primary(*pos)
          .hint("available positions: `grouped` (default) or `stacked`")
          .emit(ctx);
      }
    }
  }

  // Get resolution for x-limit validation
  auto resolution = ctx.get(res);

  // Validate x_min type
  if (auto xmin = ctx.get(x_min)) {
    validate_x_limit_type(*xmin, resolution, ctx);
  }

  // Validate x_max type
  if (auto xmax = ctx.get(x_max)) {
    validate_x_limit_type(*xmax, resolution, ctx);
  }

  // Validate x_min and x_max are compatible
  if (auto xmin = ctx.get(x_min)) {
    if (auto xmax = ctx.get(x_max)) {
      auto const min_idx = xmin->inner.get_data().index();
      auto const max_idx = xmax->inner.get_data().index();
      if (min_idx != max_idx) {
        diagnostic::error("`x_min` and `x_max` must have the same type")
          .primary(*xmin)
          .primary(*xmax)
          .emit(ctx);
      }
      if (xmin->inner >= xmax->inner) {
        diagnostic::error("`x_min` must be less than `x_max`")
          .primary(*xmin)
          .primary(*xmax)
          .emit(ctx);
      }
    }
  }

  // Validate y_min type
  if (auto ymin = ctx.get(y_min)) {
    validate_y_limit_type(*ymin, ctx);
  }

  // Validate y_max type
  if (auto ymax = ctx.get(y_max)) {
    validate_y_limit_type(*ymax, ctx);
  }

  // Validate y_min and y_max are compatible
  if (auto ymin = ctx.get(y_min)) {
    if (auto ymax = ctx.get(y_max)) {
      auto const ymin_coerced = to_double(ymin->inner);
      auto const ymax_coerced = to_double(ymax->inner);
      if (ymin_coerced.get_data().index() != ymax_coerced.get_data().index()) {
        diagnostic::error("`y_min` and `y_max` must have the same type")
          .primary(*ymin)
          .primary(*ymax)
          .emit(ctx);
      }
      if (ymin->inner >= ymax->inner) {
        diagnostic::error("`y_min` must be less than `y_max`")
          .primary(*ymin)
          .primary(*ymax)
          .emit(ctx);
      }
    }
  }

  // Validate fill
  if (auto f = ctx.get(fill)) {
    if (! ctx.get_location(res)) {
      diagnostic::error("`fill` cannot be specified without `resolution`")
        .primary(*f)
        .emit(ctx);
    }
    validate_y_limit_type(*f, ctx);

    // Check fill type matches y_min or y_max
    auto const f_coerced = to_double(f->inner);
    if (auto ymin = ctx.get(y_min)) {
      if (to_double(ymin->inner).get_data().index()
          != f_coerced.get_data().index()) {
        diagnostic::error("`fill` has a different type from `y_min`")
          .primary(*f)
          .primary(*ymin)
          .emit(ctx);
      }
    } else if (auto ymax = ctx.get(y_max)) {
      if (to_double(ymax->inner).get_data().index()
          != f_coerced.get_data().index()) {
        diagnostic::error("`fill` has a different type from `y_max`")
          .primary(*f)
          .primary(*ymax)
          .emit(ctx);
      }
    }
  }

  return {};
}

} // namespace

class PluginArea final : public virtual OperatorPlugin {
  auto name() const -> std::string override {
    return fmt::format("tql2.chart_area");
  }

  auto describe() const -> Description override {
    constexpr auto Ty = chart_type::area;
    auto d = tenzir::Describer<ChartArgs<Ty>, Chart<Ty>>{};

    d.named("x", &ChartArgs<Ty>::x);
    d.named("y", &ChartArgs<Ty>::y, "any");
    auto x_min = d.named("x_min", &ChartArgs<Ty>::x_min);
    auto x_max = d.named("x_max", &ChartArgs<Ty>::x_max);
    auto y_min = d.named("y_min", &ChartArgs<Ty>::y_min);
    auto y_max = d.named("y_max", &ChartArgs<Ty>::y_max);
    auto res = d.named("resolution", &ChartArgs<Ty>::res);
    auto fill = d.named("fill", &ChartArgs<Ty>::fill);
    d.named("x_log", &ChartArgs<Ty>::x_log);
    d.named("y_log", &ChartArgs<Ty>::y_log);
    d.named("group", &ChartArgs<Ty>::group, "any");
    auto position = d.named_optional("position", &ChartArgs<Ty>::position);
    auto limit = d.named_optional("_limit", &ChartArgs<Ty>::limit);

    d.validate([=](DescribeCtx& ctx) -> Empty {
      return validate_chart_common(limit, position, x_min, x_max, y_min, y_max,
                                   res, fill, ctx);
    });

    return d.without_optimize();
  }
};

class PluginBar final : public virtual OperatorPlugin {
  auto name() const -> std::string override {
    return fmt::format("tql2.chart_bar");
  }

  auto describe() const -> Description override {
    constexpr auto Ty = chart_type::bar;
    auto d = tenzir::Describer<ChartArgs<Ty>, Chart<Ty>>{};

    d.named({"x", "label"}, &ChartArgs<Ty>::x);
    d.named({"y", "value"}, &ChartArgs<Ty>::y, "any");
    auto x_min = d.named("x_min", &ChartArgs<Ty>::x_min);
    auto x_max = d.named("x_max", &ChartArgs<Ty>::x_max);
    auto y_min = d.named("y_min", &ChartArgs<Ty>::y_min);
    auto y_max = d.named("y_max", &ChartArgs<Ty>::y_max);
    auto res = d.named("resolution", &ChartArgs<Ty>::res);
    auto fill = d.named("fill", &ChartArgs<Ty>::fill);
    d.named("x_log", &ChartArgs<Ty>::x_log);
    d.named("y_log", &ChartArgs<Ty>::y_log);
    d.named("group", &ChartArgs<Ty>::group, "any");
    auto position = d.named_optional("position", &ChartArgs<Ty>::position);
    auto limit = d.named_optional("_limit", &ChartArgs<Ty>::limit);

    d.validate([=](DescribeCtx& ctx) -> Empty {
      return validate_chart_common(limit, position, x_min, x_max, y_min, y_max,
                                   res, fill, ctx);
    });

    return d.without_optimize();
  }
};

class PluginLine final : public virtual OperatorPlugin {
  auto name() const -> std::string override {
    return fmt::format("tql2.chart_line");
  }

  auto describe() const -> Description override {
    constexpr auto Ty = chart_type::line;
    auto d = tenzir::Describer<ChartArgs<Ty>, Chart<Ty>>{};

    d.named("x", &ChartArgs<Ty>::x);
    d.named("y", &ChartArgs<Ty>::y, "any");
    auto x_min = d.named("x_min", &ChartArgs<Ty>::x_min);
    auto x_max = d.named("x_max", &ChartArgs<Ty>::x_max);
    auto y_min = d.named("y_min", &ChartArgs<Ty>::y_min);
    auto y_max = d.named("y_max", &ChartArgs<Ty>::y_max);
    auto res = d.named("resolution", &ChartArgs<Ty>::res);
    auto fill = d.named("fill", &ChartArgs<Ty>::fill);
    d.named("x_log", &ChartArgs<Ty>::x_log);
    d.named("y_log", &ChartArgs<Ty>::y_log);
    d.named("group", &ChartArgs<Ty>::group, "any");
    auto limit = d.named_optional("_limit", &ChartArgs<Ty>::limit);

    // Line chart doesn't have position,  so pass limit as a dummy (ctx.get will
    // return nullopt)
    d.validate([=](DescribeCtx& ctx) -> Empty {
      return validate_chart_common(limit, limit, x_min, x_max, y_min, y_max,
                                   res, fill, ctx);
    });

    return d.without_optimize();
  }
};

class PluginPie final : public virtual OperatorPlugin {
  auto name() const -> std::string override {
    return fmt::format("tql2.chart_pie");
  }

  auto describe() const -> Description override {
    constexpr auto Ty = chart_type::pie;
    auto d = tenzir::Describer<ChartArgs<Ty>, Chart<Ty>>{};

    d.named({"x", "label"}, &ChartArgs<Ty>::x);
    d.named({"y", "value"}, &ChartArgs<Ty>::y, "any");
    d.named("group", &ChartArgs<Ty>::group, "any");
    auto limit = d.named_optional("_limit", &ChartArgs<Ty>::limit);

    // Pie chart only has limit validation
    d.validate([=](DescribeCtx& ctx) -> Empty {
      return validate_chart_pie(limit, ctx);
    });

    return d.without_optimize();
  }
};

} // namespace tenzir::plugins::chart

TENZIR_REGISTER_PLUGIN(tenzir::plugins::chart::PluginArea)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::chart::PluginBar)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::chart::PluginLine)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::chart::PluginPie)
