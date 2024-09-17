//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/eval.hpp"

#include <tenzir/aggregation_function.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::min {

namespace {

template <basic_type Type>
class min_function final : public aggregation_function {
public:
  explicit min_function(type input_type) noexcept
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
    if (!min_ || caf::get<view_type>(view) < *min_) {
      min_ = materialize(caf::get<view_type>(view));
    }
  }

  [[nodiscard]] caf::expected<data> finish() && override {
    return data{min_};
  }

  std::optional<type_to_data_t<Type>> min_ = {};
};

class min_instance final : public aggregation_instance {
public:
  using min_t = variant<caf::none_t, int64_t, uint64_t, double, duration>;
  explicit min_instance(ast::expression expr) : expr_{std::move(expr)} {
  }

  auto update(const table_slice& input, session ctx) -> void override {
    if (min_ and std::holds_alternative<caf::none_t>(min_.value())) {
      return;
    }
    auto arg = eval(expr_, input, ctx);
    if (not type_) {
      type_ = arg.type;
    }
    const auto warn = [&](const auto&) -> min_t {
      diagnostic::warning("expected `{}`, got `{}`", type_, arg.type)
        .primary(expr_)
        .emit(ctx);
      return caf::none_t{};
    };
    auto f = detail::overload{
      [](const arrow::NullArray&) {},
      [&]<class T>(const T& array)
        requires numeric_type<type_from_arrow_t<T>>
      {
        for (auto i = int64_t{}; i < array.length(); ++i) {
          if (array.IsValid(i)) {
            const auto val = array.Value(i);
            if (not min_) {
              min_ = val;
              continue;
            }
            min_ = min_->match(
              warn,
              [&](std::integral auto& self) -> min_t {
                if constexpr (std::same_as<T, arrow::DoubleArray>) {
                  return std::min(static_cast<double>(self), val);
                } else {
                  if (std::cmp_less(val, self)) {
                    return val;
                  }
                  return self;
                }
              },
              [&](double self) -> min_t {
                return std::min(self, static_cast<double>(val));
              });
            if (std::holds_alternative<caf::none_t>(min_.value())) {
              return;
            }
          }
        }
      },
      [&](const arrow::DurationArray& array) {
        for (auto i = int64_t{}; i < array.length(); ++i) {
          if (array.IsValid(i)) {
            const auto val = array.Value(i);
            if (not min_) {
              min_ = duration{val};
            }
            min_ = min_->match(warn, [&](duration self) -> min_t {
              return duration{std::min(self.count(), val)};
            });
            if (std::holds_alternative<caf::none_t>(min_.value())) {
              return;
            }
          }
        }
      },
      [&](const auto&) {
        diagnostic::warning("expected types `int`, `uint`, "
                            "`double` or `duration`, got `{}`",
                            arg.type)
          .primary(expr_)
          .emit(ctx);
        min_ = caf::none_t{};
      }};
    caf::visit(f, *arg.array);
  }

  auto finish() -> data override {
    if (min_) {
      return min_->match([](auto min) {
        return data{min};
      });
    }
    return data{};
  }

private:
  ast::expression expr_;
  type type_;
  std::optional<min_t> min_;
};

class plugin : public virtual aggregation_function_plugin,
               public virtual aggregation_plugin {
  caf::error initialize([[maybe_unused]] const record& plugin_config,
                        [[maybe_unused]] const record& global_config) override {
    return {};
  }

  [[nodiscard]] std::string name() const override {
    return "min";
  };

  [[nodiscard]] caf::expected<std::unique_ptr<aggregation_function>>
  make_aggregation_function(const type& input_type) const override {
    auto f = detail::overload{
      [&]<basic_type Type>(
        const Type&) -> caf::expected<std::unique_ptr<aggregation_function>> {
        return std::make_unique<min_function<Type>>(input_type);
      },
      []<complex_type Type>(const Type& type)
        -> caf::expected<std::unique_ptr<aggregation_function>> {
        return caf::make_error(ec::invalid_configuration,
                               fmt::format("min aggregation function does not "
                                           "support complex type {}",
                                           type));
      },
    };
    return caf::visit(f, input_type);
  }

  auto make_aggregation(invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function("tql2.min")
          .add(expr, "<expr>")
          .parse(inv, ctx));
    return std::make_unique<min_instance>(std::move(expr));
  }

  auto aggregation_default() const -> data override {
    return caf::none;
  }
};

} // namespace

} // namespace tenzir::plugins::min

TENZIR_REGISTER_PLUGIN(tenzir::plugins::min::plugin)
