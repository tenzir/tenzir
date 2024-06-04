//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/aggregation_function.hpp>
#include <tenzir/detail/string_literal.hpp>
#include <tenzir/plugin.hpp>

#include <arrow/util/tdigest.h>

namespace tenzir::plugins::quantile {

namespace {

template <basic_type Type>
class quantile_function final : public aggregation_function {
public:
  explicit quantile_function(type input_type, double percentile) noexcept
    : aggregation_function(std::move(input_type)),
      percentile_{percentile},
      tdigest_{} {
    // nop
  }

private:
  auto output_type() const -> type override {
    return input_type();
  }

  auto add(const data_view& view) -> void override {
    using view_type = tenzir::view<type_to_data_t<Type>>;
    if (caf::holds_alternative<caf::none_t>(view)) {
      return;
    }
    const auto x = static_cast<double>(caf::get<view_type>(view));
    if constexpr (std::is_same_v<Type, double_type>) {
      if (std::isnan(x)) {
        return;
      }
    }
    tdigest_.Add(x);
  }

  auto add(const arrow::Array& array) -> void override {
    const auto& typed_array = caf::get<type_to_arrow_array_t<Type>>(array);
    for (auto&& value : values(Type{}, typed_array)) {
      if (not value) {
        continue;
      }
      if constexpr (std::is_same_v<Type, double_type>) {
        if (std::isnan(*value)) {
          continue;
        }
      }
      tdigest_.Add(static_cast<double>(*value));
    }
  }

  auto finish() && -> caf::expected<data> override {
    if (tdigest_.is_empty()) {
      return data{};
    }
    auto quantile = tdigest_.Quantile(percentile_);
    if (not std::is_same_v<Type, double_type>) {
      quantile = std::round(quantile);
    }
    return data{static_cast<type_to_data_t<Type>>(quantile)};
  }

  const double percentile_;
  arrow::internal::TDigest tdigest_;
};

template <detail::string_literal Name, uint8_t Percentile>
  requires(Percentile > 0 and Percentile < 100)
class plugin : public virtual aggregation_function_plugin {
  auto name() const -> std::string override {
    return std::string{Name.str()};
  };

  auto make_aggregation_function(const type& input_type) const
    -> caf::expected<std::unique_ptr<aggregation_function>> override {
    const auto percentile = Percentile / 100.0;
    auto f = detail::overload{
      [&](const uint64_type&)
        -> caf::expected<std::unique_ptr<aggregation_function>> {
        return std::make_unique<quantile_function<uint64_type>>(input_type,
                                                                percentile);
      },
      [&](const int64_type&)
        -> caf::expected<std::unique_ptr<aggregation_function>> {
        return std::make_unique<quantile_function<int64_type>>(input_type,
                                                               percentile);
      },
      [&](const double_type&)
        -> caf::expected<std::unique_ptr<aggregation_function>> {
        return std::make_unique<quantile_function<double_type>>(input_type,
                                                                percentile);
      },
      [](const concrete_type auto& type)
        -> caf::expected<std::unique_ptr<aggregation_function>> {
        return caf::make_error(ec::invalid_configuration,
                               fmt::format("{} aggregation "
                                           "function does not "
                                           "support type {}",
                                           Name.str(), type));
      },
    };
    return caf::visit(f, input_type);
  }

  auto aggregation_default() const -> data override {
    return caf::none;
  }
};

using median_plugin = plugin<"median", 50>;
using p50_plugin = plugin<"p50", 50>;
using p75_plugin = plugin<"p75", 75>;
using p90_plugin = plugin<"p90", 90>;
using p95_plugin = plugin<"p95", 95>;
using p99_plugin = plugin<"p99", 99>;

} // namespace

} // namespace tenzir::plugins::quantile

TENZIR_REGISTER_PLUGIN(tenzir::plugins::quantile::median_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::quantile::p50_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::quantile::p75_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::quantile::p90_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::quantile::p95_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::quantile::p99_plugin)
