//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/aggregation_function.hpp>
#include <tenzir/plugin.hpp>

namespace tenzir::plugins::stddev_variance {

namespace {

enum class mode {
  stddev,
  variance,
};

template <basic_type Type>
class function final : public aggregation_function {
public:
  explicit function(type input_type, enum mode mode) noexcept
    : aggregation_function(std::move(input_type)), mode_{mode} {
    // nop
  }

private:
  auto output_type() const -> type override {
    return type{double_type{}};
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
    count_ += 1;
    mean_ += (x - mean_) / count_;
    mean_squared_ += ((x * x) - mean_squared_) / count_;
  }

  void add(const arrow::Array& array) override {
    const auto& typed_array = caf::get<type_to_arrow_array_t<Type>>(array);
    for (auto&& value : values(Type{}, typed_array)) {
      if (not value) {
        continue;
      }
      const auto x = static_cast<double>(*value);
      if constexpr (std::is_same_v<Type, double_type>) {
        if (std::isnan(x)) {
          continue;
        }
      }
      count_ += 1;
      mean_ += (x - mean_) / count_;
      mean_squared_ += ((x * x) - mean_squared_) / count_;
    }
  }

  auto finish() && -> caf::expected<data> override {
    if (count_ == 0) {
      return data{};
    }
    const auto variance = mean_squared_ - (mean_ * mean_);
    return data{mode_ == mode::stddev ? std::sqrt(variance) : variance};
  }

  double mean_ = {};
  double mean_squared_ = {};
  size_t count_ = {};
  mode mode_ = {};
};

template <mode Mode>
class plugin : public virtual aggregation_function_plugin {
  auto name() const -> std::string override {
    return Mode == mode::stddev ? "stddev" : "variance";
  };

  [[nodiscard]] caf::expected<std::unique_ptr<aggregation_function>>
  make_aggregation_function(const type& input_type) const override {
    auto f = detail::overload{
      [&](const uint64_type&)
        -> caf::expected<std::unique_ptr<aggregation_function>> {
        return std::make_unique<function<uint64_type>>(input_type, Mode);
      },
      [&](const int64_type&)
        -> caf::expected<std::unique_ptr<aggregation_function>> {
        return std::make_unique<function<int64_type>>(input_type, Mode);
      },
      [&](const double_type&)
        -> caf::expected<std::unique_ptr<aggregation_function>> {
        return std::make_unique<function<double_type>>(input_type, Mode);
      },
      [](const concrete_type auto& type)
        -> caf::expected<std::unique_ptr<aggregation_function>> {
        return caf::make_error(
          ec::invalid_configuration,
          fmt::format("aggregation function does not support type {}", type));
      },
    };
    return caf::visit(f, input_type);
  }

  auto aggregation_default() const -> data override {
    return {};
  }
};

using stddev_plugin = plugin<mode::stddev>;
using variance_plugin = plugin<mode::variance>;

} // namespace

} // namespace tenzir::plugins::stddev_variance

TENZIR_REGISTER_PLUGIN(tenzir::plugins::stddev_variance::stddev_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::stddev_variance::variance_plugin)
