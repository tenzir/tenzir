//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arrow_time_utils.hpp"
#include "tenzir/arrow_utils.hpp"
#include "tenzir/async.hpp"
#include "tenzir/cast.hpp"
#include "tenzir/concept/printable/tenzir/json.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/enumerate.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/detail/stable_set.hpp"
#include "tenzir/nova/aggregation.hpp"
#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/data_array_builder.hpp"
#include "tenzir/nova/eval.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/nova/materialize.hpp"
#include "tenzir/nova/record_array_builder.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/resolve.hpp"
#include "tenzir/variant.hpp"

#include <ranges>
#include <string_view>

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

using OrderedGroupMap = std::map<data, GroupedBucket>;
using CategoricalGroupMap = detail::stable_map<data, GroupedBucket>;

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
  Option<ast::expression> group;
  Option<located<data>> x_min;
  Option<located<data>> x_max;
  Option<located<data>> y_min;
  Option<located<data>> y_max;
  Option<located<duration>> res;
  Option<located<data>> fill;
  Option<location> x_log;
  Option<location> y_log;
  located<uint64_t> limit{
    Ty == chart_type::bar or Ty == chart_type::pie ? 100u : 100'000u,
    location::unknown,
  };
  located<std::string> position{"grouped", location::unknown};
};

auto make_once_entity() -> ast::entity {
  return ast::entity{
    std::vector{ast::identifier{"once", location::unknown}},
  };
}

auto find_plugins(call_map const& y, session ctx) -> plugins_map {
  auto result = plugins_map{};
  auto const entity = make_once_entity();
  for (auto const& [_, call] : y) {
    if (auto const* ptr
        = dynamic_cast<aggregation_plugin const*>(&ctx.reg().get(call))) {
      result.emplace_back(ptr, call);
      continue;
    }
    auto wrapped_call
      = ast::function_call{entity, {call}, call.get_location(), false};
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
    auto inv = function_invocation{arg};
    auto instance = plugin->make_aggregation(std::move(inv), ctx);
    TENZIR_ASSERT(instance);
    b.instances.push_back(std::move(instance).unwrap());
  }
  return b;
}

template <chart_type Ty>
auto handle_y(call_map& y_out, location& y_loc_out,
              Option<located<duration>> const& res, ast::expression& y,
              session ctx) -> failure_or<void> {
  auto const entity = make_once_entity();
  y_loc_out = y.get_location();
  return match(
    y,
    [&](ast::record& rec) -> failure_or<void> {
      // Structural validation is done in Describer::validate; assert invariants.
      TENZIR_ASSERT(not rec.items.empty());
      TENZIR_ASSERT(Ty != chart_type::pie or rec.items.size() == 1);
      for (auto& i : rec.items) {
        auto* field = try_as<ast::record::field>(i);
        TENZIR_ASSERT(field); // spreads caught by validate
        auto const loc = field->expr.get_location();
        TRY(match(
          field->expr,
          [&](ast::function_call& call) -> failure_or<void> {
            y_out[field->name.name] = std::move(call);
            return {};
          },
          [&](auto& expr) -> failure_or<void> {
            TENZIR_ASSERT(not res); // caught by validate
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
      TENZIR_ASSERT(not res); // caught by validate
      auto yname = std::string{};
      if (auto ss = ast::field_path::try_from(y)) {
        yname = fmt::format("{}", fmt::join(ss->path()
                                              | std::ranges::views::transform(
                                                &ast::field_path::segment::id)
                                              | std::ranges::views::transform(
                                                &ast::identifier::name),
                                            "."));
      } else if (Ty == chart_type::pie) {
        yname = "value";
      } else {
        yname = "y";
      }
      auto const loc = y.get_location();
      auto result = ast::function_call{entity, {std::move(y)}, loc, false};
      TENZIR_ASSERT(resolve_entities(result, ctx));
      y_out[yname] = std::move(result);
      return {};
    });
}

template <chart_type Ty>
auto handle_xlimit(ChartArgs<Ty> const& args, ast::binary_op op,
                   located<data> limit) -> failure_or<xlimit> {
  auto const& loc = limit.source;
  auto result = match(
    limit.inner,
    [](const caf::none_t&) -> failure_or<ast::constant> {
      TENZIR_UNREACHABLE(); // caught by validate
    },
    [](pattern const&) -> failure_or<ast::constant> {
      TENZIR_UNREACHABLE(); // caught by validate
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
      return ast::constant{*view_at<time_type>(*result, 0), loc};
    },
    [&](auto const& d) -> failure_or<ast::constant> {
      return ast::constant{d, loc};
    });
  TRY(auto c, result);
  auto expr = ast::binary_expr{args.x.inner(), op, c};
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
  Option<xlimit> x_min;
  Option<xlimit> x_max;
  location y_loc;
  std::string xpath;
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
    if (not prep_) {
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
  auto prepare(OpCtx& ctx) -> Option<Prepared> {
    auto sp = session_provider::make(ctx.dh());
    auto s = sp.as_session();
    auto prep = Prepared{};

    auto y_copy = args_.y;
    if (auto result = handle_y<Ty>(prep.y, prep.y_loc, args_.res, y_copy, s);
        not result) {
      return None{};
    }

    if (args_.x_min) {
      auto result = handle_xlimit(args_, ast::binary_op::geq, *args_.x_min);
      if (not result) {
        return None{};
      }
      prep.x_min = std::move(*result);
    }
    if (args_.x_max) {
      auto result = handle_xlimit(args_, ast::binary_op::leq, *args_.x_max);
      if (not result) {
        return None{};
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

    prep.xpath = args_.x.path()[0].id.name;
    for (auto i = size_t{1}; i < args_.x.path().size(); ++i) {
      prep.xpath += ".";
      prep.xpath += args_.x.path()[i].id.name;
    }

    return prep;
  }

  auto name() const -> std::string {
    return fmt::format("chart_{}", to_string(Ty));
  }

  auto process_slice(table_slice const& slice, session ctx) -> void {
    auto& dh = ctx.dh();
    auto xs = eval(args_.x, slice, dh);
    auto gs = get_group_strings(slice, dh);
    TENZIR_ASSERT(gs.type.kind().template is<string_type>());
    if (not xty_) {
      if (not validate_x(xs.type, dh)) {
        return;
      }
      xty_ = xs.type;
      if (is_categorical(xs.type)) {
        groups_ = CategoricalGroupMap{};
      } else {
        groups_ = OrderedGroupMap{};
      }
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
        return;
      }
    }
    if (args_.res) {
      xs = floor(xs);
    }
    match(groups_, [&](auto& groups) {
      auto* current_bucket = static_cast<Bucket*>(nullptr);
      auto run_begin = size_t{0};
      auto flush = [&](size_t run_end) {
        if (not current_bucket or run_begin == run_end) {
          return;
        }
        for (auto&& instance : current_bucket->instances) {
          instance->update(subslice(slice, run_begin, run_end), ctx);
        }
      };
      for (auto&& [idx, x] : detail::enumerate<int64_t>(xs.values())) {
        auto const row = detail::narrow<size_t>(idx);
        if constexpr (Ty != chart_type::bar and Ty != chart_type::pie) {
          if (is<caf::none_t>(x)) {
            flush(row);
            current_bucket = nullptr;
            run_begin = row + 1;
            diagnostic::warning("x-axis cannot be `null`")
              .primary(args_.x)
              .emit(ctx);
            continue;
          }
        }
        auto group_name = std::string{};
        if (gs.array->IsNull(idx)) {
          if (args_.group) {
            diagnostic::warning("got group name `null`")
              .primary(*args_.group)
              .note("using `\"null\"` instead")
              .emit(dh);
            group_name = "null";
          }
        } else {
          group_name = *view_at<string_type>(*gs.array, idx);
        }
        auto x_value = materialize(x);
        auto group_it = groups.find(x_value);
        if (group_it == groups.end()) {
          flush(row);
          current_bucket = nullptr;
          run_begin = row;
          if (groups.size() == args_.limit.inner) {
            diagnostic::warning("got more than {} data points",
                                args_.limit.inner)
              .primary(args_.x)
              .note("skipping excess data points")
              .hint("consider filtering data or aggregating over a bigger "
                    "`resolution`")
              .emit(ctx);
            run_begin = row + 1;
            continue;
          }
          group_it = groups.try_emplace(std::move(x_value)).first;
        }
        auto& group = group_it->second.groups;
        auto bucket_it = group.find(group_name);
        if (bucket_it == group.end()) {
          flush(row);
          current_bucket = nullptr;
          run_begin = row;
          bucket_it
            = group.try_emplace(group_name, make_bucket(prep_->plugins, ctx))
                .first;
        }
        auto* bucket = &bucket_it->second;
        if (bucket != current_bucket) {
          flush(row);
          current_bucket = bucket;
          run_begin = row;
        }
      }
      flush(slice.rows());
    });
  }

  auto build_output(diagnostic_handler& dh) -> std::vector<table_slice> {
    if (match(groups_, [](auto const& groups) {
          return groups.empty();
        })) {
      diagnostic::warning("chart_{} received no valid data", to_string(Ty))
        .primary(args_.x)
        .emit(dh);
      return {};
    }
    auto const& xpath = prep_->xpath;
    // Collect unique group names in insertion order
    auto all_group_names = detail::stable_set<std::string>{};
    match(groups_, [&](auto const& groups) {
      for (auto const& [_, gb] : groups) {
        for (auto const& [gname, _] : gb.groups) {
          all_group_names.emplace(gname);
        }
      }
    });
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
    match(
      groups_,
      [&](OrderedGroupMap const& groups) {
        if (prep_->x_min and args_.res) {
          TENZIR_ASSERT(not groups.empty());
          auto min = Option{prep_->x_min->rounded};
          auto const& first = groups.begin()->first;
          if (*min != first) {
            fill_at(*min);
          }
          while (auto gap = find_gap(min, first)) {
            min = gap.value();
            fill_at(std::move(gap).value());
          }
        }
        for (auto prev = Option<data>{};
             auto const& [x, gb] :
             groups | std::views::take(args_.limit.inner)) {
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
          TENZIR_ASSERT(not groups.empty());
          auto last = Option{groups.rbegin()->first};
          auto const& max = prep_->x_max->rounded;
          while (auto gap = find_gap(last, max)) {
            last = gap.value();
            fill_at(std::move(gap).value());
          }
          if (*last != max) {
            fill_at(max);
          }
        }
      },
      [&](CategoricalGroupMap const& groups) {
        for (auto const& [x, gb] : groups) {
          insert(x, gb);
        }
      });
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
    auto const attrs = make_attributes(xpath, ynames, limits);
    auto result = std::vector<table_slice>{};
    for (auto&& slice : slices) {
      auto attr_views = std::vector<type::attribute_view>{};
      attr_views.reserve(attrs.size());
      for (const auto& [key, value] : attrs) {
        attr_views.push_back({key, value});
      }
      auto schema = type{slice.schema(), std::move(attr_views)};
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
    auto gss = eval(*args_.group, slice, dh);
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
          .primary(*args_.group)
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

  auto filter_input(table_slice slice, diagnostic_handler& dh) -> table_slice {
    // Early exit: no limits set
    if (not prep_->x_min and not prep_->x_max) {
      return slice;
    }
    // Build combined expression from x_min and x_max
    auto expr = ast::expression{};
    if (prep_->x_min and prep_->x_max) {
      expr = ast::binary_expr{
        prep_->x_min->expr,
        ast::binary_op::and_,
        prep_->x_max->expr,
      };
    } else if (prep_->x_min) {
      expr = prep_->x_min->expr;
    } else {
      TENZIR_ASSERT(prep_->x_max);
      expr = prep_->x_max->expr;
    }
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

  auto find_gap(Option<data> const& prev, data const& curr) const
    -> Option<data> {
    if (not prev) {
      return None{};
    }
    if (is<caf::none_t>(*prev) or is<caf::none_t>(curr)) {
      return None{};
    }
    auto result = match(
      std::tie(curr, *prev),
      [&](duration const& c, duration const& p) -> Option<data> {
        if (c - p > args_.res->inner) {
          return p + args_.res->inner;
        }
        return None{};
      },
      [&](time const& c, time const& p) -> Option<data> {
        if (c - p > args_.res->inner) {
          return p + args_.res->inner;
        }
        return None{};
      },
      [](auto const&, auto const&) -> Option<data> {
        TENZIR_UNREACHABLE();
      });
    return result;
  }

  auto make_attributes(
    std::string const& xpath,
    detail::stable_map<std::string, bool> const& ynames,
    detail::stable_map<std::string_view, std::string> const& limits) const
    -> std::vector<std::pair<std::string, std::string>> {
    auto attrs = std::vector<std::pair<std::string, std::string>>{
      {"chart", std::string{to_string(Ty)}},
      {"x", xpath},
    };
    // pie charts do not support position or axis-type options
    if constexpr (Ty != chart_type::pie) {
      attrs.emplace_back("position", args_.position.inner);
      attrs.emplace_back("x_axis_type", args_.x_log ? "log" : "linear");
      attrs.emplace_back("y_axis_type", args_.y_log ? "log" : "linear");
    }
    for (auto const& [name, value] : limits) {
      if (not value.empty()) {
        attrs.emplace_back(name, value);
      }
    }
    // Build the y-axis attribute keys: "y" for the first series, then "y1",
    // "y2", ... for any additional ones. The count must cover both the number
    // of aggregation expressions and the number of resolved output columns.
    auto ynums = std::deque<std::string>{"y"};
    auto const total = std::max(prep_->y.size(), ynames.size());
    for (auto i = ynums.size(); i < total; ++i) {
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

  auto is_categorical(type const& ty) const -> bool {
    if constexpr (Ty == chart_type::bar or Ty == chart_type::pie) {
      return ty.kind().is_any<null_type, ip_type, subnet_type, string_type>();
    }
    return false;
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
  Option<Prepared> prep_;
  // state
  Option<type> xty_;
  variant<OrderedGroupMap, CategoricalGroupMap> groups_ = OrderedGroupMap{};
};

// The chart's groups retain their first-seen order within each x value.
struct EventBucket {
  std::vector<Box<nova::AggregationState>> states;
  std::vector<nova::storage::Index> rows;
};

struct EventGroups {
  detail::stable_map<std::string, EventBucket> groups;
};

template <chart_type Ty>
class ChartEvents final : public Operator<nova::Events, nova::Events> {
public:
  explicit ChartEvents(ChartArgs<Ty> args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if (ctx.checkpoint_settings()) {
      diagnostic::error("`chart_{}` does not support checkpointing with "
                        "`--nova` yet",
                        to_string(Ty))
        .primary(args_.x)
        .emit(ctx);
      co_return;
    }
    if (args_.res) {
      args_.res->inner = duration{std::abs(args_.res->inner.count())};
    }
    auto sp = session_provider::make(ctx.dh());
    auto calls = call_map{};
    auto loc = location{};
    auto y = args_.y;
    if (not handle_y<Ty>(calls, loc, args_.res, y, sp.as_session())) {
      co_return;
    }
    y_loc_ = loc;
    for (auto const& [name, call] : calls) {
      auto invocation = ast::expression{call};
      if (not dynamic_cast<aggregation_plugin const*>(
            &sp.as_session().reg().get(call))) {
        invocation = ast::function_call{
          make_once_entity(), {invocation}, call.get_location(), false};
        if (not resolve_entities(invocation, sp.as_session())) {
          co_return;
        }
      }
      auto aggregation
        = co_await nova::Aggregation::make(std::move(invocation), ctx);
      if (not aggregation) {
        co_return;
      }
      names_.push_back(name);
      aggregations_.push_back(std::move(*aggregation));
    }
    x_name_ = args_.x.path()[0].id.name;
    for (auto i = size_t{1}; i < args_.x.path().size(); ++i) {
      x_name_ += ".";
      x_name_ += args_.x.path()[i].id.name;
    }
    auto x = co_await nova::Evaluator::make(args_.x.inner(), ctx);
    if (not x) {
      co_return;
    }
    x_.emplace(std::move(*x));
    if (args_.group) {
      auto group = co_await nova::Evaluator::make(*args_.group, ctx);
      if (not group) {
        co_return;
      }
      group_.emplace(std::move(*group));
    }
    if (args_.x_min) {
      auto limit = handle_xlimit(args_, ast::binary_op::geq, *args_.x_min);
      if (not limit) {
        co_return;
      }
      x_min_ = std::move(limit->rounded);
      auto evaluator
        = co_await nova::Evaluator::make(std::move(limit->expr), ctx);
      if (not evaluator) {
        co_return;
      }
      x_min_filter_.emplace(std::move(*evaluator));
    }
    if (args_.x_max) {
      auto limit = handle_xlimit(args_, ast::binary_op::leq, *args_.x_max);
      if (not limit) {
        co_return;
      }
      x_max_ = std::move(limit->rounded);
      auto evaluator
        = co_await nova::Evaluator::make(std::move(limit->expr), ctx);
      if (not evaluator) {
        co_return;
      }
      x_max_filter_.emplace(std::move(*evaluator));
    }
    ready_ = true;
  }

  auto process(nova::Events input, Push<nova::Events>&, OpCtx& ctx)
    -> Task<void> override {
    if (not ready_ or not input.mask.any()) {
      co_return;
    }
    // Evaluate bounds with the regular comparison semantics before bucketing.
    for (auto* limit : {&x_min_filter_, &x_max_filter_}) {
      if (not *limit) {
        continue;
      }
      auto predicate = (*limit)
                         ->eval(input, nova::EvalCtx{ctx.dh()})
                         .template get_alternative<nova::Bool>();
      if (not predicate) {
        co_return;
      }
      input.mask = input.mask & predicate->present
                   & as<nova::storage::BitMap>(predicate->data.storage());
      if (not input.mask.any()) {
        co_return;
      }
    }
    auto xs = x_->eval(input, nova::EvalCtx{ctx.dh()});
    auto admitted = nova::storage::BitMap::Mutable{input.length()};
    auto selected = std::vector<std::pair<nova::storage::Index, data>>{};
    for (auto row : nova::storage::true_bits(input.mask)) {
      auto x = nova::materialize_legacy(xs.get(row));
      if (not valid_x(x, ctx.dh())) {
        continue;
      }
      if (args_.res and not is<caf::none_t>(x)) {
        auto const step = std::abs(args_.res->inner.count());
        if (auto* d = try_as<duration>(&x)) {
          auto const n = d->count();
          auto const remainder = n % step;
          x = duration{n - (remainder < 0 ? remainder + step : remainder)};
        } else if (auto* t = try_as<time>(&x)) {
          auto const n = t->time_since_epoch().count();
          auto const remainder = n % step;
          x = time{
            duration{n - (remainder < 0 ? remainder + step : remainder)}};
        }
      }
      // The two maps differ in ordering, not in the buckets they store.
      auto admit = [&](auto& map) -> bool {
        if (map.find(x) != map.end()) {
          return true;
        }
        if (map.size() >= args_.limit.inner) {
          diagnostic::warning("got more than {} data points", args_.limit.inner)
            .primary(args_.x)
            .note("skipping excess data points")
            .hint("consider filtering data or aggregating over a bigger "
                  "`resolution`")
            .emit(ctx);
          return false;
        }
        map.try_emplace(x);
        return true;
      };
      if (not(categorical_ ? admit(categorical_groups_)
                           : admit(ordered_groups_))) {
        continue;
      }
      admitted.set(row, true);
      selected.emplace_back(row, std::move(x));
    }
    input.mask = std::move(admitted).finish();
    if (not input.mask.any()) {
      co_return;
    }
    auto gs = group_ ? Option{group_->eval(input, nova::EvalCtx{ctx.dh()})}
                     : Option<nova::Array<nova::Data>>{None{}};
    auto touched = std::vector<std::pair<data, std::string>>{};
    for (auto const& [row, x] : selected) {
      auto group_name = std::string{};
      if (gs) {
        auto value = nova::materialize_legacy(gs->get(row));
        if (is<caf::none_t>(value)) {
          diagnostic::warning("got group name `null`")
            .primary(*args_.group)
            .note("using `\"null\"` instead")
            .emit(ctx);
          group_name = "null";
        } else if (auto* str = try_as<std::string>(&value)) {
          group_name = *str;
        } else if (is<int64_t>(value) or is<uint64_t>(value)
                   or is<double>(value)) {
          group_name = fmt::to_string(value);
        } else {
          diagnostic::warning("cannot group this type")
            .primary(*args_.group)
            .emit(ctx);
          group_name = "null";
        }
      }
      auto& groups
        = categorical_ ? categorical_groups_.at(x) : ordered_groups_.at(x);
      auto& members = groups.groups;
      auto member = members.find(group_name);
      if (member == members.end()) {
        auto fresh = EventBucket{};
        for (auto const& aggregation : aggregations_) {
          fresh.states.push_back(aggregation->make_state());
        }
        member = members.try_emplace(group_name, std::move(fresh)).first;
      }
      auto& bucket = member->second;
      if (bucket.rows.empty()) {
        touched.emplace_back(x, group_name);
      }
      bucket.rows.push_back(row);
    }
    auto buckets = std::vector<EventBucket*>{};
    buckets.reserve(touched.size());
    for (auto const& [x, group] : touched) {
      auto& groups
        = categorical_ ? categorical_groups_.at(x) : ordered_groups_.at(x);
      buckets.push_back(&groups.groups.at(group));
    }
    for (auto index = size_t{0}; index < aggregations_.size(); ++index) {
      auto groups = std::vector<nova::AggregationGroup>{};
      for (auto* bucket : buckets) {
        groups.push_back({*bucket->states[index], bucket->rows});
      }
      aggregations_[index]->update(input, groups, nova::EvalCtx{ctx.dh()});
    }
    for (auto* bucket : buckets) {
      bucket->rows.clear();
    }
  }

  auto finalize(Push<nova::Events>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    if (not ready_) {
      co_return FinalizeBehavior::done;
    }
    if (ordered_groups_.empty() and categorical_groups_.empty()) {
      diagnostic::warning("chart_{} received no valid data", to_string(Ty))
        .primary(args_.x)
        .emit(ctx);
      co_return FinalizeBehavior::done;
    }
    auto ynames = detail::stable_map<std::string, bool>{};
    auto group_names = detail::stable_set<std::string>{};
    auto collect = [&](auto const& groups) {
      for (auto const& [_, gb] : groups) {
        for (auto const& [group, bucket] : gb.groups) {
          group_names.emplace(group);
        }
      }
    };
    if (categorical_) {
      collect(categorical_groups_);
    } else {
      collect(ordered_groups_);
    }
    auto name_for = [&](std::string_view group, std::string_view name) {
      if (not args_.group) {
        return std::string{name};
      }
      if (names_.size() == 1) {
        return std::string{group};
      }
      return fmt::format("{}_{}", group, name);
    };
    auto fill = args_.fill ? to_double(args_.fill->inner) : data{};
    auto warned_y_bounds = detail::stable_set<std::string>{};
    auto validate_names = [&](auto const& groups) {
      for (auto const& [_, gb] : groups) {
        for (auto const& [group, bucket] : gb.groups) {
          for (auto i = size_t{0}; i < names_.size(); ++i) {
            auto value
              = to_double(nova::materialize_legacy(bucket.states[i]->get()));
            if (args_.fill and is<caf::none_t>(value)) {
              value = fill;
            }
            auto const yname = name_for(group, names_[i]);
            auto valid = is<caf::none_t>(value) or is<double>(value)
                         or is<duration>(value);
            if (valid and (args_.y_min or args_.y_max)) {
              auto const& limit = args_.y_min ? args_.y_min : args_.y_max;
              auto const converted = to_double(limit->inner);
              auto const value_type = type::infer(value);
              auto const limit_type = type::infer(converted);
              valid = value_type == limit_type;
              if (not valid and value_type and limit_type
                  and warned_y_bounds.emplace(yname).second) {
                diagnostic::warning("limit has a different type `{}` from `y` "
                                    "type `{}`",
                                    limit_type->kind(), value_type->kind())
                  .primary(limit->source)
                  .note(fmt::format("skipping {}", yname))
                  .emit(ctx);
              }
            }
            auto [it, inserted] = ynames.try_emplace(yname, valid);
            if (not inserted) {
              it->second &= valid;
            }
          }
        }
      }
    };
    if (categorical_) {
      validate_names(categorical_groups_);
    } else {
      validate_names(ordered_groups_);
    }
    auto builder = nova::ArrayBuilder<nova::Record>{};
    // The first row is a chart specification, not a point.
    auto spec = builder.record().field("chart").record();
    spec.field("kind").data(to_string(Ty));
    spec.field("x").data(x_name_);
    if constexpr (Ty != chart_type::pie) {
      spec.field("position").data(args_.position.inner);
      spec.field("x_axis_type").data(args_.x_log ? "log" : "linear");
      spec.field("y_axis_type").data(args_.y_log ? "log" : "linear");
    }
    auto add_limit
      = [&](std::string_view name, Option<located<data>> const& limit) {
          if (limit) {
            spec.field(name).data(jsonify_limit(limit->inner));
          }
        };
    add_limit("x_min", args_.x_min);
    add_limit("x_max", args_.x_max);
    if (args_.y_min) {
      spec.field("y_min").data(jsonify_limit(to_double(args_.y_min->inner)));
    }
    if (args_.y_max) {
      spec.field("y_max").data(jsonify_limit(to_double(args_.y_max->inner)));
    }
    auto index = size_t{0};
    for (auto const& [name, valid] : ynames) {
      if (valid) {
        spec.field(index == 0 ? "y" : fmt::format("y{}", index)).data(name);
        ++index;
      }
    }
    // A nullable point still belongs to the same typed column as its peers.
    // Carry the column types with the spec so serve can reconstruct its schema
    // even when the points arrive on separate pages.
    auto column_types = detail::stable_map<std::string, std::string>{};
    auto collect_types = [&](auto const& groups) {
      for (auto const& [_, gb] : groups) {
        for (auto const& [group, bucket] : gb.groups) {
          for (auto i = size_t{0}; i < names_.size(); ++i) {
            auto value
              = to_double(nova::materialize_legacy(bucket.states[i]->get()));
            auto kind = is<double>(value)     ? "double"
                        : is<duration>(value) ? "duration"
                                              : "";
            if (kind[0] == '\0') {
              continue;
            }
            auto [it, inserted]
              = column_types.try_emplace(name_for(group, names_[i]), kind);
            if (not inserted and it->second != kind) {
              it->second.clear();
            }
          }
        }
      }
    };
    if (categorical_) {
      collect_types(categorical_groups_);
    } else {
      collect_types(ordered_groups_);
    }
    // A nullable x column needs the same schema on every serve page.
    if (x_type_) {
      auto const& kind = x_type_->kind();
      auto x_kind = std::string_view{};
      if (kind.is<int64_type>()) {
        x_kind = "int64";
      } else if (kind.is<uint64_type>()) {
        x_kind = "uint64";
      } else if (kind.is<double_type>()) {
        x_kind = "double";
      } else if (kind.is<duration_type>()) {
        x_kind = "duration";
      } else if (kind.is<time_type>()) {
        x_kind = "time";
      } else if (kind.is<string_type>()) {
        x_kind = "string";
      } else if (kind.is<ip_type>()) {
        x_kind = "ip";
      } else if (kind.is<subnet_type>()) {
        x_kind = "subnet";
      }
      if (not x_kind.empty()) {
        column_types.try_emplace(x_name_, x_kind);
      }
    }
    if (not column_types.empty()) {
      auto types = spec.field("types").record();
      for (auto const& [name, kind] : column_types) {
        if (not kind.empty()) {
          types.field(name).data(kind);
        }
      }
    }
    auto insert = [&](data const& x, EventGroups const* gb) {
      auto record = builder.record();
      nova::append_legacy_data(record.field(x_name_), x, ctx.dh());
      for (auto const& group : group_names) {
        auto const* bucket = static_cast<EventBucket const*>(nullptr);
        if (gb) {
          if (auto it = gb->groups.find(group); it != gb->groups.end()) {
            bucket = &it->second;
          }
        }
        for (auto i = size_t{0}; i < names_.size(); ++i) {
          auto value
            = bucket
                ? to_double(nova::materialize_legacy(bucket->states[i]->get()))
                : fill;
          if (args_.fill and is<caf::none_t>(value)) {
            value = fill;
          }
          auto valid = is<caf::none_t>(value) or is<double>(value)
                       or is<duration>(value);
          if (not valid) {
            diagnostic::warning("y-axis cannot have this type")
              .primary(y_loc_)
              .emit(ctx);
          }
          nova::append_legacy_data(record.field(name_for(group, names_[i])),
                                   value, ctx.dh());
        }
      }
    };
    if (categorical_) {
      for (auto const& [x, gb] : categorical_groups_) {
        insert(x, &gb);
      }
    } else {
      auto prev = Option<data>{};
      auto gap_until = [&](data const& next) {
        if (not args_.res or not prev) {
          return;
        }
        auto const step = duration{std::abs(args_.res->inner.count())};
        while (true) {
          auto gap = match(
            std::tie(next, *prev),
            [&](duration const& n, duration const& p) -> Option<data> {
              if (n - p > step) {
                return p + step;
              }
              return None{};
            },
            [&](time const& n, time const& p) -> Option<data> {
              if (n - p > step) {
                return p + step;
              }
              return None{};
            },
            [](auto const&, auto const&) -> Option<data> {
              return None{};
            });
          if (not gap) {
            break;
          }
          insert(*gap, nullptr);
          prev = std::move(*gap);
        }
      };
      if (x_min_ and args_.res) {
        prev = *x_min_;
        if (*prev != ordered_groups_.begin()->first) {
          insert(*prev, nullptr);
        }
      }
      for (auto const& [x, gb] : ordered_groups_) {
        gap_until(x);
        insert(x, &gb);
        prev = x;
      }
      if (x_max_ and args_.res) {
        gap_until(*x_max_);
        if (*prev != *x_max_) {
          insert(*x_max_, nullptr);
        }
      }
    }
    auto length = builder.length();
    co_await push(
      nova::Events{builder.finish(), nova::storage::BitMap{length, true},
                   nova::Events::Meta::make_empty(length, "tenzir.chart")});
    co_return FinalizeBehavior::done;
  }

private:
  auto valid_x(data const& x, diagnostic_handler& dh) -> bool {
    auto valid = is<int64_t>(x) or is<uint64_t>(x) or is<double>(x)
                 or is<duration>(x) or is<time>(x);
    if constexpr (Ty == chart_type::bar or Ty == chart_type::pie) {
      valid |= is<caf::none_t>(x) or is<ip>(x) or is<subnet>(x)
               or is<std::string>(x);
    }
    if (not valid
        or (args_.res
            and not(is<duration>(x) or is<time>(x) or is<caf::none_t>(x)))) {
      diagnostic::warning("x-axis cannot have this type")
        .primary(args_.x)
        .note("skipping invalid events")
        .emit(dh);
      return false;
    }
    if (is<caf::none_t>(x)) {
      return true;
    }
    if (not x_type_) {
      x_type_ = type::infer(x);
      categorical_ = is<ip>(x) or is<subnet>(x) or is<std::string>(x);
      if (categorical_ and not ordered_groups_.empty()) {
        // Leading null buckets belong before the first categorical value.
        TENZIR_ASSERT(ordered_groups_.size() == 1);
        auto it = ordered_groups_.begin();
        TENZIR_ASSERT(is<caf::none_t>(it->first));
        categorical_groups_.try_emplace(it->first, std::move(it->second));
        ordered_groups_.clear();
      }
    } else if (type::infer(x) != x_type_) {
      diagnostic::warning("cannot plot different types on the x-axis")
        .primary(args_.x)
        .note("skipping invalid events")
        .emit(dh);
      return false;
    }
    return true;
  }

  ChartArgs<Ty> args_;
  bool ready_ = false;
  bool categorical_ = false;
  Option<type> x_type_;
  Option<nova::Evaluator> x_;
  Option<nova::Evaluator> group_;
  Option<nova::Evaluator> x_min_filter_;
  Option<nova::Evaluator> x_max_filter_;
  std::vector<Box<nova::Aggregation>> aggregations_;
  std::vector<std::string> names_;
  std::string x_name_;
  location y_loc_;
  Option<data> x_min_;
  Option<data> x_max_;
  std::map<data, EventGroups> ordered_groups_;
  detail::stable_map<data, EventGroups> categorical_groups_;
};

// Helper validation for x limit types
auto validate_x_limit_type(located<data> const& d,
                           Option<located<duration>> const& res,
                           DescribeCtx& ctx) -> void {
  if (is<caf::none_t>(d.inner)) {
    diagnostic::error("limit cannot be `null`").primary(d).emit(ctx);
    return;
  }
  if (is<pattern>(d.inner)) {
    diagnostic::error("limit cannot be a pattern").primary(d).emit(ctx);
    return;
  }
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
  if (res and not t->kind().is_any<time_type, duration_type>()) {
    diagnostic::error("cannot group type `{}` with resolution", t->kind())
      .primary(d)
      .primary(res->source)
      .emit(ctx);
    return;
  }
}

// Validation for the y expression of non-pie charts (area, bar, line)
auto validate_y_common(ast::expression const& y,
                       Option<located<duration>> const& res, DescribeCtx& ctx)
  -> void {
  match(
    y,
    [&](ast::record const& rec) {
      if (rec.items.empty()) {
        diagnostic::error("requires at least one value").primary(y).emit(ctx);
        return;
      }
      for (auto const& item : rec.items) {
        auto const* field = try_as<ast::record::field>(item);
        if (not field) {
          diagnostic::error("cannot use `...` here").primary(y).emit(ctx);
          return;
        }
        if (not try_as<ast::function_call>(field->expr) and res) {
          diagnostic::error("an aggregation function is required if "
                            "`resolution` is specified")
            .primary(field->expr)
            .emit(ctx);
        }
      }
    },
    [](ast::function_call const&) {},
    [&](auto const&) {
      if (res) {
        diagnostic::error(
          "an aggregation function is required if resolution is specified")
          .primary(y)
          .emit(ctx);
      }
    });
}

// Helper validation for y limit types
auto validate_y_limit_type(located<data> const& d, DescribeCtx& ctx) -> void {
  auto const t = type::infer(d.inner);
  if (not t) {
    diagnostic::error("failed to infer type of option").primary(d).emit(ctx);
    return;
  }
  if (not t->kind()
            .is_any<int64_type, uint64_type, double_type, duration_type>()) {
    diagnostic::error("y-axis cannot have type `{}`", t->kind())
      .primary(d)
      .emit(ctx);
    return;
  }
}

// Validates the `position` argument for chart types that support it (area, bar).
auto validate_position(auto position, DescribeCtx& ctx) -> void {
  if (auto pos = ctx.get(position)) {
    if (pos->inner != "stacked" and pos->inner != "grouped") {
      diagnostic::error("unsupported `position`")
        .primary(*pos)
        .hint("available positions: `grouped` (default) or `stacked`")
        .emit(ctx);
    }
  }
}

// Unified validation function for area, bar, line charts.
// Does not validate `position`; call validate_position() separately for chart
// types that support it.
auto validate_chart_common(auto y, auto limit, auto x_min, auto x_max,
                           auto y_min, auto y_max, auto res, auto fill,
                           DescribeCtx& ctx) -> Empty {
  // Validate y expression
  auto resolution = ctx.get(res);
  if (resolution) {
    if (resolution->inner.count() == 0) {
      diagnostic::error("`resolution` must be non-zero")
        .primary(*resolution)
        .emit(ctx);
    }
  }
  if (auto y_val = ctx.get(y)) {
    validate_y_common(*y_val, resolution, ctx);
  }
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
      } else if (xmin->inner >= xmax->inner) {
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
      } else if (ymin_coerced >= ymax_coerced) {
        diagnostic::error("`y_min` must be less than `y_max`")
          .primary(*ymin)
          .primary(*ymax)
          .emit(ctx);
      }
    }
  }

  // Validate fill
  if (auto f = ctx.get(fill)) {
    if (not ctx.get_location(res)) {
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
    return fmt::format("chart_area");
  }

  auto describe() const -> Description override {
    constexpr auto Ty = chart_type::area;
    auto d = tenzir::Describer<ChartArgs<Ty>, Chart<Ty>, ChartEvents<Ty>>{};

    d.named("x", &ChartArgs<Ty>::x);
    auto y = d.named("y", &ChartArgs<Ty>::y, "any");
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
      validate_position(position, ctx);
      return validate_chart_common(y, limit, x_min, x_max, y_min, y_max, res,
                                   fill, ctx);
    });

    return d.without_optimize();
  }
};

class PluginBar final : public virtual OperatorPlugin {
  auto name() const -> std::string override {
    return fmt::format("chart_bar");
  }

  auto describe() const -> Description override {
    constexpr auto Ty = chart_type::bar;
    auto d = tenzir::Describer<ChartArgs<Ty>, Chart<Ty>, ChartEvents<Ty>>{};

    d.named({"x", "label"}, &ChartArgs<Ty>::x);
    auto y = d.named({"y", "value"}, &ChartArgs<Ty>::y, "any");
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
      validate_position(position, ctx);
      return validate_chart_common(y, limit, x_min, x_max, y_min, y_max, res,
                                   fill, ctx);
    });

    return d.without_optimize();
  }
};

class PluginLine final : public virtual OperatorPlugin {
  auto name() const -> std::string override {
    return fmt::format("chart_line");
  }

  auto describe() const -> Description override {
    constexpr auto Ty = chart_type::line;
    auto d = tenzir::Describer<ChartArgs<Ty>, Chart<Ty>, ChartEvents<Ty>>{};

    d.named("x", &ChartArgs<Ty>::x);
    auto y = d.named("y", &ChartArgs<Ty>::y, "any");
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

    d.validate([=](DescribeCtx& ctx) -> Empty {
      return validate_chart_common(y, limit, x_min, x_max, y_min, y_max, res,
                                   fill, ctx);
    });

    return d.without_optimize();
  }
};

class PluginPie final : public virtual OperatorPlugin {
  auto name() const -> std::string override {
    return fmt::format("chart_pie");
  }

  auto describe() const -> Description override {
    constexpr auto Ty = chart_type::pie;
    auto d = tenzir::Describer<ChartArgs<Ty>, Chart<Ty>, ChartEvents<Ty>>{};

    d.named({"x", "label"}, &ChartArgs<Ty>::x);
    auto y = d.named({"y", "value"}, &ChartArgs<Ty>::y, "any");
    d.named("group", &ChartArgs<Ty>::group, "any");
    auto limit = d.named_optional("_limit", &ChartArgs<Ty>::limit);

    d.validate([=](DescribeCtx& ctx) -> Empty {
      if (auto y_val = ctx.get(y)) {
        match(
          *y_val,
          [&](ast::record const& rec) {
            if (rec.items.size() != 1) {
              diagnostic::error("`chart_pie` requires exactly one value")
                .primary(*y_val)
                .emit(ctx);
            } else if (try_as<ast::spread>(rec.items[0])) {
              diagnostic::error("cannot use `...` here")
                .primary(*y_val)
                .emit(ctx);
            }
          },
          [](ast::function_call const&) {}, [](auto const&) {});
      }
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
    });

    return d.without_optimize();
  }
};

} // namespace tenzir::plugins::chart

TENZIR_REGISTER_PLUGIN(tenzir::plugins::chart::PluginArea)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::chart::PluginBar)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::chart::PluginLine)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::chart::PluginPie)
