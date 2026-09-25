//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/nova/aggregation/value_counts.hpp>
#include <tenzir/nova/materialize.hpp>
#include <tenzir/plugin/register.hpp>

#include <algorithm>
#include <functional>
#include <numeric>
#include <vector>

#include "value_counts_instance.hpp"

namespace tenzir::plugins::value_counts {

namespace {

class instance final : public detail::value_counts_instance {
public:
  explicit instance(ast::expression expr)
    : value_counts_instance{std::move(expr), "value_counts"} {
  }

  auto get() const -> data override {
    auto result = list{};
    result.reserve(state().counts().size());
    for (auto const& [value, count] : state().counts()) {
      result.emplace_back(record{
        {"value", value},
        {"count", count},
      });
    }
    std::ranges::sort(result, std::less<>{}, [](auto const& item) {
      return as_vector(as<record>(item))[0].second;
    });
    return result;
  }
};

using nova_value_counts::ValueArgs;
using nova_value_counts::ValueCounts;
using nova_value_counts::ValueCountsFunction;

/// The distinct values with their counts, sorted by value like the legacy
/// implementation.
struct GetValueCounts {
  auto operator()(ValueCounts const& counts) const -> nova::Data {
    auto const values = counts.values();
    auto order = std::vector<size_t>(values.size());
    std::iota(order.begin(), order.end(), size_t{0});
    auto keys = std::vector<data>{};
    keys.reserve(values.size());
    for (auto const* value : values) {
      keys.push_back(nova::materialize(nova::RowView<nova::Data>{*value}));
    }
    std::ranges::stable_sort(order, std::less<>{}, [&](size_t i) {
      return keys[i];
    });
    auto result = nova::List{};
    result.reserve(values.size());
    for (auto i : order) {
      auto entry = nova::Record{};
      entry["value"] = *values[i];
      entry["count"] = nova::Data{counts.counts()[i]};
      result.push_back(nova::Data{std::move(entry)});
    }
    return nova::Data{std::move(result)};
  }
};

class value_counts_plugin final : public virtual aggregation_plugin,
                                  public virtual nova::AggregationPlugin {
public:
  auto name() const -> std::string override {
    return "value_counts";
  }

  auto describe() const -> nova::AggregationDescription override {
    auto d = nova::AggregationDescriber<ValueArgs,
                                        ValueCountsFunction<GetValueCounts>>{};
    d.positional("x", &ValueArgs::x, "any");
    return std::move(d).finish();
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_aggregation(function_invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    auto parser = argument_parser2::function(name());
    parser.positional("x", expr, "any");
    TRY(parser.parse(inv, ctx));
    return std::make_unique<instance>(std::move(expr));
  }
};

} // namespace

} // namespace tenzir::plugins::value_counts

using namespace tenzir::plugins::value_counts;
TENZIR_REGISTER_PLUGIN(value_counts_plugin)
