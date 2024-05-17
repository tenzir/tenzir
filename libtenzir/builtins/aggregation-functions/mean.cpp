//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/aggregation_function.hpp>
#include <tenzir/plugin.hpp>

namespace tenzir::plugins::mean {

namespace {

template <basic_type Type>
class mean_function final : public aggregation_function {
public:
  explicit mean_function(type input_type) noexcept
    : aggregation_function(std::move(input_type)) {
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
      count_ += 1;
      mean_ += (static_cast<double>(*value) - mean_) / count_;
    }
  }

  auto finish() && -> caf::expected<data> override {
    if (count_ == 0) {
      return data{};
    }
    return data{mean_};
  }

  double mean_ = {};
  size_t count_ = {};
};

class plugin : public virtual aggregation_function_plugin {
  auto name() const -> std::string override {
    return "mean";
  };

  auto make_aggregation_function(const type& input_type) const
    -> caf::expected<std::unique_ptr<aggregation_function>> override {
    auto f = detail::overload{
      [&](const uint64_type&)
        -> caf::expected<std::unique_ptr<aggregation_function>> {
        return std::make_unique<mean_function<uint64_type>>(input_type);
      },
      [&](const int64_type&)
        -> caf::expected<std::unique_ptr<aggregation_function>> {
        return std::make_unique<mean_function<int64_type>>(input_type);
      },
      [&](const double_type&)
        -> caf::expected<std::unique_ptr<aggregation_function>> {
        return std::make_unique<mean_function<double_type>>(input_type);
      },
      [](const concrete_type auto& type)
        -> caf::expected<std::unique_ptr<aggregation_function>> {
        return caf::make_error(ec::invalid_configuration,
                               fmt::format("mean aggregation function does not "
                                           "support type {}",
                                           type));
      },
    };
    return caf::visit(f, input_type);
  }

  auto aggregation_default() const -> data override {
    return caf::none;
  }
};

} // namespace

} // namespace tenzir::plugins::mean

TENZIR_REGISTER_PLUGIN(tenzir::plugins::mean::plugin)
