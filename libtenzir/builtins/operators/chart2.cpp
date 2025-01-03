//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arrow_time_utils.hpp"
#include "tenzir/arrow_utils.hpp"
#include "tenzir/cast.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/zip_iterator.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/plugin.hpp"

namespace tenzir::plugins::chart {

TENZIR_ENUM(chart_type, line, area, bar, pie);

namespace {

using bucket = std::vector<std::unique_ptr<aggregation_instance>>;
using bucket_map = std::map<data, bucket>;

struct chart_args {
  chart_type ty;
  ast::expression x;
  ast::record y;
  std::optional<std::pair<expression, ast::expression>> from;
  std::optional<std::pair<expression, ast::expression>> to;
  std::optional<located<duration>> resolution;
  std::optional<location> x_log;
  std::optional<location> y_log;
  std::optional<location> stacked;

  friend auto inspect(auto& f, chart_args& x) -> bool {
    return f.object(x)
      .pretty_name("chart_args")
      .fields(f.field("ty", x.ty), f.field("x", x.x), f.field("y", x.y),
              f.field("x_log", x.x_log), f.field("y_log", x.y_log),
              f.field("from", x.from), f.field("to", x.to),
              f.field("resolution", x.resolution),
              f.field("stacked", x.stacked));
  }

  auto validate(diagnostic_handler& dh) const -> bool {
    bool success = true;
    const auto it = std::ranges::find_if_not(y.items, [](auto&& x) {
      return is<ast::record::field>(x);
    });
    if (it != end(y.items)) {
      diagnostic::error("cannot use `...` here").primary(y).emit(dh);
      success = false;
    }
    return success;
  }

  auto make_bucket(session ctx) const -> bucket {
    auto b = bucket{};
    // TODO: Find and store plugins once.
    const auto* def = plugins::find<aggregation_plugin>("first");
    TENZIR_ASSERT(def);
    for (auto&& item : y.items) {
      auto&& i = as<ast::record::field>(item);
      match(
        i.expr,
        [&](const ast::function_call& call) {
          const auto* fn
            = dynamic_cast<const aggregation_plugin*>(&ctx.reg().get(call));
          if (fn) {
            auto result = fn->make_aggregation(
              aggregation_plugin::invocation{std::move(call)}, ctx);
            TENZIR_ASSERT(result);
            b.push_back(std::move(result).unwrap());
            return;
          }
          auto result = def->make_aggregation(
            aggregation_plugin::invocation{
              {{}, {std::move(i.expr)}, location::unknown, false}},
            ctx);
          TENZIR_ASSERT(result);
          b.push_back(std::move(result).unwrap());
        },
        [&](const auto&) {
          auto result = def->make_aggregation(
            aggregation_plugin::invocation{
              {{}, {std::move(i.expr)}, location::unknown, false}},
            ctx);
          TENZIR_ASSERT(result);
          b.push_back(std::move(result).unwrap());
        });
    }
    return b;
  }
};

class chart_operator2 final : public crtp_operator<chart_operator2> {
public:
  chart_operator2() = default;

  explicit chart_operator2(chart_args args) : args_{std::move(args)} {
  }

  auto
  get_bucket(bucket_map& map, const data_view x, session ctx) const -> bucket* {
    // PERF: Maybe we only need to materialize when inserting new
    const auto id = materialize(x);
    if (auto it = map.find(id); it != map.end()) {
      return &it->second;
    }
    const auto [it, success] = map.emplace(id, args_.make_bucket(ctx));
    TENZIR_ASSERT(success);
    return &it->second;
  }

  auto filter(const chart_args& args, generator<table_slice> input,
              diagnostic_handler& dh) const -> generator<table_slice> {
    const auto expr = std::invoke([&]() -> ast::expression {
      if (args.from and args.to) {
        return ast::binary_expr{args.from->second,
                                {ast::binary_op::and_, location::unknown},
                                args.to->second};
      }
      if (args.from) {
        return args.from->second;
      }
      TENZIR_ASSERT(args.to);
      return args.to->second;
    });
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      const auto fs = eval(expr, slice, dh);
      // Modified from `where`
      auto offset = int64_t{0};
      for (auto& filter : eval(expr, slice, dh)) {
        const auto array = try_as<arrow::BooleanArray>(&*filter.array);
        TENZIR_ASSERT(array);
        const auto len = array->length();
        if (array->true_count() == len) {
          co_yield subslice(slice, offset, offset + len);
          offset += len;
          continue;
        }
        // if (array->true_count() == 0) {
        //   co_yield {};
        //   offset += len;
        //   continue;
        // }
        auto curr = array->Value(0);
        auto begin = int64_t{0};
        // We add an artificial `false` at index `length` to flush.
        auto results = std::vector<table_slice>{};
        for (auto i = int64_t{1}; i < len + 1; ++i) {
          const auto next = i != len && array->IsValid(i) && array->Value(i);
          if (curr == next) {
            continue;
          }
          if (curr) {
            results.push_back(subslice(slice, offset + begin, offset + i));
          }
          curr = next;
          begin = i;
        }
        co_yield concatenate(std::move(results));
        offset += len;
      }
    }
  }

  auto
  operator()(generator<table_slice> input,
             operator_control_plane& ctrl) const -> generator<table_slice> {
    auto& dh = ctrl.diagnostics();
    const auto attrs = make_attributes();
    auto xty = std::optional<type>{};
    auto buckets = bucket_map{};
    auto sp = session_provider::make(dh);
    auto s = sp.as_session();
    if (args_.from or args_.to) {
      input = filter(args_, std::move(input), dh);
    }
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      auto xss = eval(args_.x, slice, dh);
      auto consumed = size_t{};
      for (auto&& xs : xss.parts()) {
        if (not xty) {
          if (not validate_xtype(xs.type, dh)) {
            consumed += xs.length();
            continue;
          }
          xty = xs.type;
        }
        if (xs.type != xty.value()) {
          diagnostic::warning("cannot plot different types `{}` and `{}` on "
                              "the X-axis",
                              xty.value().kind(), xs.type.kind())
            .primary(args_.x)
            .note("skipping invalid events")
            .emit(dh);
          consumed += xs.length();
          continue;
        }
        if (args_.resolution) {
          xs = floor_xs(xs);
        }
        bucket* b = nullptr;
        for (auto i = size_t{}; auto&& x : xs.values()) {
          auto* newb = get_bucket(buckets, x, s);
          if (b != newb) {
            for (auto&& instance : b ? *b : *newb) {
              // HACK: This check is necessary because `subslice` does not
              // forward the schema if `begin == end`.
              if (consumed != consumed + i) {
                instance->update(subslice(slice, consumed, consumed + i), s);
              }
            }
            b = newb;
            consumed += i;
            i = 0;
          }
          ++i;
        }
        for (auto&& instance : *b) {
          if (consumed != slice.rows()) {
            instance->update(subslice(slice, consumed, slice.rows()), s);
          }
        }
      }
    }
    auto b = series_builder{};
    const data* prev = nullptr;
    const auto* null_bucket
      = args_.resolution ? get_bucket(buckets, caf::none, s) : nullptr;
    const auto insert = [&](data x, const bucket& bucket) {
      auto r = b.record();
      r.field("x").data(std::move(x));
      for (auto&& [item, instance] : detail::zip_equal(args_.y.items, bucket)) {
        r.field(as<ast::record::field>(item).name.name).data(instance->get());
      }
    };
    for (const auto& [k, v] : buckets) {
      if (is<caf::none_t>(k)) {
        continue;
      }
      if (auto gap = find_gap(k, prev)) {
        insert(std::move(gap).value(), *null_bucket);
      }
      insert(k, v);
      prev = &k;
    }
    // FIXME: `series_builder` is not merging int/uint & double?
    for (auto&& slice : b.finish_as_table_slice()) {
      co_yield cast(slice, {slice.schema(), std::vector{attrs}});
    }
  }

  auto
  find_gap(const data& curr, const data* prev) const -> std::optional<data> {
    // FIXME: Frontend does not re-join lines after a `null` value.
    return std::nullopt;
    if (not args_.resolution or not prev) {
      return std::nullopt;
    }
    return match(
      std::tie(curr, *prev),
      [&](const duration& c, const duration& p) -> std::optional<data> {
        if (c - p > args_.resolution->inner) {
          return p + args_.resolution->inner;
        }
        return std::nullopt;
      },
      [&](const time& c, const time& p) -> std::optional<data> {
        if (c - p > args_.resolution->inner) {
          return p + args_.resolution->inner;
        }
        return std::nullopt;
      },
      [](const auto&, const auto&) -> std::optional<data> {
        TENZIR_UNREACHABLE();
      });
  }

  auto make_attributes() const -> std::vector<type::attribute_view> {
    auto attrs = std::vector<type::attribute_view>{
      {"chart", to_string(args_.ty)},
      {"position", args_.stacked ? "stacked" : "grouped"},
      {"x_axis_type", args_.x_log ? "log" : "linear"},
      {"y_axis_type", args_.y_log ? "log" : "linear"},
      {"x", "x"},
    };
    for (auto i = ys.size(); i < args_.y.items.size(); ++i) {
      ys.emplace_back(fmt::format("y{}", i));
    }
    for (const auto& [y, item] : detail::zip(ys, args_.y.items)) {
      const auto& i = as<ast::record::field>(item);
      attrs.emplace_back(y, i.name.name);
    }
    return attrs;
  }

  auto validate_xtype(const type& ty, diagnostic_handler& dh) const -> bool {
    auto valid = ty.kind()
                   .is_any<int64_type, uint64_type, double_type, duration_type,
                           time_type>();
    if (args_.ty == chart_type::bar) {
      valid = valid or ty.kind().is_any<ip_type, subnet_type>();
    }
    if (args_.ty == chart_type::pie) {
      valid = valid or ty.kind().is_any<ip_type, subnet_type, string_type>();
    }
    if (not valid) {
      diagnostic::warning("X-axis cannot have type `{}`", ty.kind())
        .note("skipping invalid events")
        .primary(args_.x)
        .emit(dh);
      return false;
    }
    if (args_.resolution and not ty.kind().is_any<time_type, duration_type>()) {
      diagnostic::warning("cannot group type `{}` with resolution", ty.kind())
        .note("skipping invalid events")
        .primary(args_.x)
        .primary(args_.resolution->source)
        .emit(dh);
      return false;
    }
    return true;
  }

  auto validate_ytype(const type& ty, diagnostic_handler& dh) const -> bool {
    if (not ty.kind()
              .is_any<int64_type, uint64_type, double_type, duration_type,
                      time_type>()) {
      diagnostic::warning("Y-axis cannot have type `{}`", ty.kind())
        .primary(args_.y)
        .emit(dh);
      return false;
    }
    return true;
  }

  // Modified from `floor()`
  auto floor_xs(const series& xs) const -> series {
    return match(
      *xs.array,
      [&](const arrow::DurationArray& array) -> series {
        auto b
          = duration_type::make_arrow_builder(arrow::default_memory_pool());
        check(b->Reserve(array.length()));
        for (auto i = int64_t{0}; i < array.length(); i++) {
          if (array.IsNull(i)) {
            check(b->AppendNull());
            continue;
          }
          const auto val = array.Value(i);
          const auto count = std::abs(args_.resolution->inner.count());
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
        auto opts = make_round_temporal_options(args_.resolution->inner);
        return {time_type{},
                check(arrow::compute::FloorTemporal(array, std::move(opts)))
                  .array_as<arrow::TimestampArray>()};
      },
      [&](const auto&) -> series {
        TENZIR_UNREACHABLE();
      });
  }

  auto name() const -> std::string override {
    return "tql2.chart";
  }

  auto optimize(const expression& filter,
                event_order) const -> optimize_result override {
    auto expr = filter;
    if (args_.from) {
      expr = conjunction{args_.from->first, std::move(expr)};
    }
    if (args_.to) {
      expr = conjunction{args_.to->first, std::move(expr)};
    }
    auto combined = normalize_and_validate(expr);
    TENZIR_ASSERT(combined);
    return {std::move(combined).value(), event_order::ordered, copy()};
  }

  friend auto inspect(auto& f, chart_operator2& x) -> bool {
    return f.object(x)
      .pretty_name("chart_operator2")
      .fields(f.field("args_", x.args_));
  }

private:
  chart_args args_;
  // Using a `deque` to guarantee reference validity after growing
  mutable std::deque<std::string> ys{"y"};
};

template <chart_type Ty>
class chart_plugin : public virtual operator_plugin2<chart_operator2> {
  auto name() const -> std::string override {
    return fmt::format("chart_{}", to_string(Ty));
  }

  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    auto args = chart_args{};
    args.ty = Ty;
    auto y = ast::expression{};
    auto from = std::optional<ast::expression>{};
    auto to = std::optional<ast::expression>{};
    auto p = argument_parser2::operator_(name());
    p.positional("x", args.x, "any");
    p.positional("y", y, "any");
    p.named("from", from, "any");
    p.named("to", to, "any");
    p.named("resolution", args.resolution);
    if constexpr (Ty != chart_type::pie) {
      p.named("x_log", args.x_log);
      p.named("y_log", args.y_log);
    }
    p.named("stacked", args.stacked);
    TRY(p.parse(inv, ctx));
    args.y = std::invoke([&] {
      if (is<ast::record>(y)) {
        return as<ast::record>(std::move(y));
      }
      const auto loc = y.get_location();
      return ast::record{loc, {ast::record::field{{"y", loc}, y}}, loc};
    });
    if (from) {
      auto loc = from->get_location();
      args.from = split_legacy_expression(ast::binary_expr{
        args.x, {ast::binary_op::gt, loc}, std::move(from).value()});
    }
    if (to) {
      auto loc = to->get_location();
      args.to = split_legacy_expression(ast::binary_expr{
        args.x, {ast::binary_op::lt, loc}, std::move(to).value()});
    }
    if (not args.validate(ctx)) {
      return failure::promise();
    }
    return std::make_unique<chart_operator2>(std::move(args));
  }
};

using chart_line = chart_plugin<chart_type::line>;
using chart_area = chart_plugin<chart_type::area>;
using chart_bar = chart_plugin<chart_type::bar>;
using chart_pie = chart_plugin<chart_type::pie>;

} // namespace

} // namespace tenzir::plugins::chart

TENZIR_REGISTER_PLUGIN(tenzir::plugins::chart::chart_line)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::chart::chart_area)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::chart::chart_bar)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::chart::chart_pie)
