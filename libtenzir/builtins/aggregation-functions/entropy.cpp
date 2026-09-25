//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/nova/aggregation/value_counts.hpp>
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

using nova_value_counts::ValueCounts;

struct EntropyArgs {
  nova::ValueArgument x;
  bool normalize = false;
};

auto entropy_of(ValueCounts const& counts, bool normalize) -> nova::Data {
  if (counts.size() <= 1) {
    return nova::Data{0.0};
  }
  auto const total = std::accumulate(counts.counts().begin(),
                                     counts.counts().end(), int64_t{0});
  auto result = 0.0;
  for (auto const count : counts.counts()) {
    auto const probability
      = static_cast<double>(count) / static_cast<double>(total);
    if (probability > 0.0) {
      result -= probability * std::log(probability);
    }
  }
  if (normalize) {
    result /= std::log(static_cast<double>(counts.size()));
  }
  return nova::Data{result};
}

class EntropyFunction final {
public:
  static auto eval(EntropyArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    return nova::aggregate_lists(
      args.x, frame,
      [&](nova::ListElements const& elements,
          nova::ArrayBuilder<nova::Data>& builder) {
        auto counts = ValueCounts{};
        counts.add(elements);
        nova::append_data(builder, entropy_of(counts, args.normalize));
      });
  }

  auto update(EntropyArgs const& args, nova::EvalFrame frame) -> void {
    normalize_ = args.normalize;
    counts_.add(args.x.data, frame.mask());
  }

  auto get() const -> nova::Data {
    return entropy_of(counts_, normalize_);
  }

  auto reset() -> void {
    counts_ = {};
  }

private:
  ValueCounts counts_;
  /// A constant argument, remembered from the first update for `get`.
  bool normalize_ = false;
};

class entropy_plugin final : public virtual aggregation_plugin,
                             public virtual nova::AggregationPlugin {
public:
  auto name() const -> std::string override {
    return "entropy";
  }

  auto describe() const -> nova::AggregationDescription override {
    auto d = nova::AggregationDescriber<EntropyArgs, EntropyFunction>{};
    d.positional("x", &EntropyArgs::x, "any");
    d.named("normalize", &EntropyArgs::normalize);
    return std::move(d).finish();
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
