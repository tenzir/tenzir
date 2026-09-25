//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/nova/aggregation/value_counts.hpp>
#include <tenzir/plugin/register.hpp>

#include <algorithm>

#include "value_counts_instance.hpp"

namespace tenzir::plugins::mode {

namespace {

class instance final : public detail::value_counts_instance {
public:
  explicit instance(ast::expression expr)
    : value_counts_instance{std::move(expr), "mode"} {
  }

  auto get() const -> data override {
    auto const comp = [](auto const& lhs, auto const& rhs) {
      return lhs.second < rhs.second;
    };
    auto const it = std::ranges::max_element(state().counts(), comp);
    return it == state().counts().end() ? data{} : it->first;
  }
};

using nova_value_counts::ValueArgs;
using nova_value_counts::ValueCounts;
using nova_value_counts::ValueCountsFunction;

/// The most frequent value; the first seen of them on a tie.
struct GetMode {
  auto operator()(ValueCounts const& counts) const -> nova::Data {
    auto const it = std::ranges::max_element(counts.counts());
    if (it == counts.counts().end()) {
      return nova::Data{};
    }
    return *counts.values()[it - counts.counts().begin()];
  }
};

class mode_plugin final : public virtual aggregation_plugin,
                          public virtual nova::AggregationPlugin {
public:
  auto name() const -> std::string override {
    return "mode";
  }

  auto describe() const -> nova::AggregationDescription override {
    auto d
      = nova::AggregationDescriber<ValueArgs, ValueCountsFunction<GetMode>>{};
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

} // namespace tenzir::plugins::mode

using namespace tenzir::plugins::mode;
TENZIR_REGISTER_PLUGIN(mode_plugin)
