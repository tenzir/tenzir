//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/plugin/register.hpp>

#include <algorithm>
#include <functional>

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

class value_counts_plugin final : public virtual aggregation_plugin {
public:
  auto name() const -> std::string override {
    return "value_counts";
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
