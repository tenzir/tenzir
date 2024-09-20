//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/aggregation_function.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

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

// TODO: This can be cleaned up probably
class stddev_variance_instance final : public aggregation_instance {
public:
  stddev_variance_instance(ast::expression expr, mode m)
    : mode_{m}, expr_{std::move(expr)} {
  }

  auto update(const table_slice& input, session ctx) -> void override {
    if (state_ == state::failed) {
      return;
    }
    auto arg = eval(expr_, input, ctx);
    auto f = detail::overload{
      [](const arrow::NullArray&) {},
      [&]<class T>(const T& array)
        requires numeric_type<type_from_arrow_t<T>>
                   or std::same_as<T, arrow::DurationArray>
      {
        if constexpr (std::same_as<T, arrow::DurationArray>) {
          if (state_ != state::dur and state_ != state::none) {
            diagnostic::warning("got incompatible types `number` and `{}`",
                                arg.type.kind())
              .primary(expr_)
              .emit(ctx);
            state_ = state::failed;
            return;
          }
          if (mode_ == mode::variance) {
            diagnostic::warning("expected `int`, `uint` or `double` got `{}`",
                                arg.type.kind())
              .primary(expr_)
              .emit(ctx);
            state_ = state::failed;
            return;
          }
          state_ = state::dur;
        } else {
          if (state_ != state::numeric and state_ != state::none) {
            diagnostic::warning("got incompatible types `duration` and `{}`",
                                arg.type.kind())
              .primary(expr_)
              .emit(ctx);
            state_ = state::failed;
            return;
          }
          state_ = state::numeric;
        }
        for (auto i = int64_t{}; i < array.length(); ++i) {
          if (array.IsValid(i)) {
            const auto x = static_cast<double>(array.Value(i));
            if constexpr (std::is_same_v<type_from_arrow_t<T>, double_type>) {
              if (std::isnan(x)) {
                continue;
              }
            }
            count_ += 1;
            mean_ += (x - mean_) / count_;
            mean_squared_ += ((x * x) - mean_squared_) / count_;
          }
        }
      },
      [&](const auto&) {
        if (mode_ == mode::variance) {
          diagnostic::warning("expected `int`, `uint` or `double` got `{}`",
                              arg.type.kind())
            .primary(expr_)
            .emit(ctx);
        } else {
          diagnostic::warning("expected `int`, `uint`, `double` or `duration`, "
                              "got `{}`",
                              arg.type.kind())
            .primary(expr_)
            .emit(ctx);
        }
        state_ = state::failed;
      }};
    caf::visit(f, *arg.array);
  }

  auto finish() -> data override {
    if (count_ == 0) {
      return data{};
    }
    const auto variance = mean_squared_ - (mean_ * mean_);
    const auto result = mode_ == mode::stddev ? std::sqrt(variance) : variance;
    switch (state_) {
      case state::none:
      case state::failed:
        return data{};
      case state::dur:
        return duration{static_cast<duration::rep>(result)};
      case state::numeric:
        return result;
    }
    TENZIR_UNREACHABLE();
  }

private:
  double mean_ = {};
  double mean_squared_ = {};
  size_t count_ = {};
  mode mode_ = {};
  enum class state { none, failed, dur, numeric } state_{state::none};
  ast::expression expr_;
};

template <mode Mode>
class plugin : public virtual aggregation_function_plugin,
               public virtual aggregation_plugin {
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

  auto make_aggregation(invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name()).add(expr, "<expr>").parse(inv, ctx));
    return std::make_unique<stddev_variance_instance>(std::move(expr), Mode);
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
