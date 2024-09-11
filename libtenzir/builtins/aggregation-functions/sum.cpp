//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/aggregation_function.hpp>
#include <tenzir/checked_math.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::sum {

namespace {

template <basic_type Type>
class sum_function final : public aggregation_function {
public:
  explicit sum_function(type input_type) noexcept
    : aggregation_function(std::move(input_type)) {
    // nop
  }

private:
  [[nodiscard]] type output_type() const override {
    TENZIR_ASSERT(caf::holds_alternative<Type>(input_type()));
    return input_type();
  }

  void add(const data_view& view) override {
    using view_type = tenzir::view<type_to_data_t<Type>>;
    if (caf::holds_alternative<caf::none_t>(view)) {
      return;
    }
    if (!sum_) {
      sum_ = materialize(caf::get<view_type>(view));
    } else {
      sum_ = *sum_ + materialize(caf::get<view_type>(view));
    }
  }

  [[nodiscard]] caf::expected<data> finish() && override {
    return data{sum_};
  }

  std::optional<type_to_data_t<Type>> sum_ = {};
};

class sum_instance : public aggregation_instance {
public:
  using sum_t = variant<caf::none_t, int64_t, uint64_t, double, duration>;
  sum_instance(ast::expression expr) : expr_{std::move(expr)} {
  }

  auto update(const table_slice& input, session ctx) -> void override {
    if (sum_ and std::holds_alternative<caf::none_t>(sum_.value())) {
      return;
    }
    auto s = eval(expr_, input, ctx);
    if (not type_) {
      type_ = s.type;
    }
    const auto warn = [&](const auto&) -> sum_t {
      diagnostic::warning("expected `{}`, got `{}`", type_, s.type)
        .primary(expr_)
        .emit(ctx);
      return caf::none_t{};
    };
    auto f = detail::overload{
      [](const arrow::NullArray&) {},
      [&]<class T>(const T& array)
        requires integral_type<type_from_arrow_t<T>>
      {
        using Type = T::value_type;
        // Int64 + UInt64 => UInt64
        // * + Double => Double
        if (not sum_) {
          sum_ = Type{};
        }
        sum_ = sum_->match(
          warn,
          [&](std::integral auto& self) -> sum_t {
            if (not std::in_range<Type>(self)) {
              diagnostic::warning("integer overflow").primary(expr_).emit(ctx);
              return caf::none_t{};
            }
            auto result = detail::narrow_cast<Type>(self);
            for (auto i = int64_t{}; i < array.length(); ++i) {
              if (array.IsValid(i)) {
                auto checked = checked_add(result, array.Value(i));
                if (not checked) {
                  diagnostic::warning("integer overflow")
                    .primary(expr_)
                    .emit(ctx);
                  return caf::none_t{};
                }
                result = checked.value();
              }
            }
            return result;
          },
          [&](double self) -> sum_t {
            for (auto i = int64_t{}; i < array.length(); ++i) {
              if (array.IsValid(i)) {
                self += static_cast<double>(array.Value(i));
              }
            }
            return self;
          });
      },
      [&](const arrow::DoubleArray& array) {
        // * => Double
        if (not sum_) {
          sum_ = double{};
        }
        sum_ = sum_->match(warn, [&](concepts::arithmetic auto& self) -> sum_t {
          auto result = static_cast<double>(self);
          for (auto i = int64_t{}; i < array.length(); ++i) {
            if (array.IsValid(i)) {
              result += array.Value(i);
            }
          }
          return result;
        });
      },
      [&](const arrow::DurationArray& array) {
        if (not sum_) {
          sum_ = duration{};
        }
        sum_ = sum_->match(warn, [&](duration self) -> sum_t {
          for (auto i = int64_t{}; i < array.length(); ++i) {
            if (array.IsValid(i)) {
              auto checked = checked_add(self.count(), array.Value(i));
              if (not checked) {
                diagnostic::warning("duration overflow")
                  .primary(expr_)
                  .emit(ctx);
                return caf::none_t{};
              }
              self += duration{array.Value(i)};
            }
          }
          return self;
        });
      },
      [&](const auto&) {
        diagnostic::warning(
          "expected `int`, `uint`, `double` or `duration`, got `{}`", s.type)
          .primary(expr_)
          .emit(ctx);
        sum_ = caf::none_t{};
      }};
    caf::visit(f, *s.array);
  }

  auto finish() -> data override {
    if (sum_) {
      return sum_->match([](auto sum) {
        return data{sum};
      });
    }
    return data{};
  }

private:
  ast::expression expr_;
  type type_;
  std::optional<sum_t> sum_;
};

class plugin : public virtual aggregation_function_plugin,
               public virtual aggregation_plugin {
  caf::error initialize([[maybe_unused]] const record& plugin_config,
                        [[maybe_unused]] const record& global_config) override {
    return {};
  }

  [[nodiscard]] std::string name() const override {
    return "sum";
  };

  [[nodiscard]] caf::expected<std::unique_ptr<aggregation_function>>
  make_aggregation_function(const type& input_type) const override {
    auto f = detail::overload{
      [&]<basic_type Type>(
        const Type&) -> caf::expected<std::unique_ptr<aggregation_function>> {
        return std::make_unique<sum_function<Type>>(input_type);
      },
      [](const time_type& type)
        -> caf::expected<std::unique_ptr<aggregation_function>> {
        return caf::make_error(ec::invalid_configuration,
                               fmt::format("sum aggregation function does not "
                                           "support type {}",
                                           type));
      },
      [](const null_type& type)
        -> caf::expected<std::unique_ptr<aggregation_function>> {
        return caf::make_error(ec::invalid_configuration,
                               fmt::format("sum aggregation function does not "
                                           "support type {}",
                                           type));
      },
      [](const string_type& type)
        -> caf::expected<std::unique_ptr<aggregation_function>> {
        return caf::make_error(ec::invalid_configuration,
                               fmt::format("sum aggregation function does not "
                                           "support type {}",
                                           type));
      },
      [](const ip_type& type)
        -> caf::expected<std::unique_ptr<aggregation_function>> {
        return caf::make_error(ec::invalid_configuration,
                               fmt::format("sum aggregation function does not "
                                           "support type {}",
                                           type));
      },
      [](const subnet_type& type)
        -> caf::expected<std::unique_ptr<aggregation_function>> {
        return caf::make_error(ec::invalid_configuration,
                               fmt::format("sum aggregation function does not "
                                           "support type {}",
                                           type));
      },
      []<complex_type Type>(const Type& type)
        -> caf::expected<std::unique_ptr<aggregation_function>> {
        return caf::make_error(ec::invalid_configuration,
                               fmt::format("sum aggregation function does not "
                                           "support complex type {}",
                                           type));
      },
    };
    return caf::visit(f, input_type);
  }

  auto aggregation_default() const -> data override {
    return caf::none;
  }

  auto make_aggregation(invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function("sum").add(expr, "<field>").parse(inv, ctx));
    return std::make_unique<sum_instance>(std::move(expr));
  }
};

} // namespace

} // namespace tenzir::plugins::sum

TENZIR_REGISTER_PLUGIN(tenzir::plugins::sum::plugin)
