//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/plugin/register.hpp>

#include <cmath>
#include <numeric>
#include <ranges>

#include "value_counts_instance.hpp"

namespace tenzir::plugins::entropy {

namespace {

class instance final : public detail::value_counts_instance {
public:
  explicit instance(ast::expression expr, bool normalize)
    : value_counts_instance{std::move(expr), "entropy"}, normalize_{normalize} {
  }

  auto get() const -> data override {
    if (state().counts().size() <= 1) {
      return 0.0;
    }
    auto result = 0.0;
    auto counts = state().counts() | std::views::values;
    // TODO: Use `std::ranges::fold_left` once supported by our libc++.
    auto const total = std::accumulate(counts.begin(), counts.end(), size_t{});
    for (auto const count : counts) {
      auto const probability = tenzir::detail::narrow<double>(count) / total;
      if (probability > 0.0) {
        result -= probability * std::log(probability);
      }
    }
    return normalize_ ? result / std::log(state().counts().size()) : result;
  }

private:
  bool const normalize_;
};

class entropy_plugin final : public virtual aggregation_plugin {
public:
  auto name() const -> std::string override {
    return "entropy";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_aggregation(function_invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    auto normalize = false;
    auto parser = argument_parser2::function(name());
    parser.positional("x", expr, "any");
    parser.named("normalize", normalize);
    TRY(parser.parse(inv, ctx));
    return std::make_unique<instance>(std::move(expr), normalize);
  }
};

} // namespace

} // namespace tenzir::plugins::entropy

using namespace tenzir::plugins::entropy;
TENZIR_REGISTER_PLUGIN(entropy_plugin)
