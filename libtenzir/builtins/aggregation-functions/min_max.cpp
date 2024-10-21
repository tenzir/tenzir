//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/aggregation_function.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/api.h>
#include <arrow/compute/api.h>

namespace tenzir::plugins::min_max {

namespace {

enum class mode {
  max,
  min,
};

template <mode Mode, basic_type Type>
class min_max_function final : public aggregation_function {
public:
  explicit min_max_function(type input_type) noexcept
    : aggregation_function(std::move(input_type)) {
    // nop
  }

private:
  auto output_type() const -> type override {
    TENZIR_ASSERT(caf::holds_alternative<Type>(input_type()));
    return input_type();
  }

  void add(const data_view& view) override {
    using view_type = tenzir::view<type_to_data_t<Type>>;
    if (caf::holds_alternative<caf::none_t>(view)) {
      return;
    }
    const auto comp = [](const auto& lhs, const auto& rhs) {
      return Mode == mode::min ? lhs < rhs : lhs > rhs;
    };
    if (not result_ or comp(caf::get<view_type>(view), *result_)) {
      result_ = materialize(caf::get<view_type>(view));
    }
  }

  auto finish() && -> caf::expected<data> override {
    return data{result_};
  }

  std::optional<type_to_data_t<Type>> result_ = {};
};

template <mode Mode>
class min_max_instance final : public aggregation_instance {
public:
  using result_t
    = variant<caf::none_t, int64_t, uint64_t, double, duration, time>;

  explicit min_max_instance(ast::expression expr) : expr_{std::move(expr)} {
  }

  auto update(const table_slice& input, session ctx) -> void override {
    if (result_ and std::holds_alternative<caf::none_t>(result_.value())) {
      return;
    }
    auto arg = eval(expr_, input, ctx);
    if (not type_) {
      type_ = arg.type;
    }
    const auto warn = [&](const auto&) -> result_t {
      diagnostic::warning("got incompatible types `{}` and `{}`", type_.kind(),
                          arg.type.kind())
        .primary(expr_)
        .emit(ctx);
      return caf::none;
    };
    // TODO: Matching on type of max_ might be better to reduce function calls
    auto f = detail::overload{
      [](const arrow::NullArray&) {},
      [&]<class T>(const T& array)
        requires numeric_type<type_from_arrow_t<T>>
                 {
                   for (auto i = int64_t{}; i < array.length(); ++i) {
                     if (array.IsValid(i)) {
                       const auto val = array.Value(i);
                       if (not result_) {
                         result_ = val;
                         continue;
                       }
                       result_ = result_->match(
                         warn,
                         [&](std::integral auto& self) -> result_t {
                           if constexpr (std::same_as<T, arrow::DoubleArray>) {
                             return Mode == mode::min
                                      ? std::min(static_cast<double>(self), val)
                                      : std::max(static_cast<double>(self),
                                                 val);
                           } else {
                             if (Mode == mode::min
                                   ? std::cmp_less(val, self)
                                   : std::cmp_greater(val, self)) {
                               return val;
                             }
                             return self;
                           }
                         },
                         [&](double self) -> result_t {
                           return Mode == mode::min
                                    ? std::min(self, static_cast<double>(val))
                                    : std::max(self, static_cast<double>(val));
                         });
                       if (std::holds_alternative<caf::none_t>(
                             result_.value())) {
                         return;
                       }
                     }
                   }
                 },
                 [&]<class T>(const T& array)
                   requires concepts::one_of<type_from_arrow_t<T>,
                                             duration_type, time_type>
      {
        using Ty = type_from_arrow_t<T>;
        for (const auto& val : values(Ty{}, array)) {
          if (val) {
            if (not result_) {
              result_ = val;
            }
            result_
              = result_->match(warn, [&](type_to_data_t<Ty> self) -> result_t {
                  return Mode == mode::min ? std::min(self, val.value())
                                           : std::max(self, val.value());
                });
            if (std::holds_alternative<caf::none_t>(result_.value())) {
              return;
            }
          }
        }
      },
      [&](const auto&) {
        diagnostic::warning("expected types `int`, `uint`, `double`, "
                            "`duration`, or `time`, but got `{}`",
                            arg.type.kind())
          .primary(expr_)
          .emit(ctx);
        result_ = caf::none;
      }};
    caf::visit(f, *arg.array);
  }

  auto finish() -> data override {
    if (result_) {
      return result_->match([](auto result) {
        return data{result};
      });
    }
    return {};
  }

private:
  ast::expression expr_ = {};
  type type_ = {};
  std::optional<result_t> result_ = {};
};

template <mode Mode>
class plugin : public virtual aggregation_function_plugin,
               public virtual aggregation_plugin {
public:
  auto name() const -> std::string override {
    return Mode == mode::min ? "min" : "max";
  };

  auto make_aggregation_function(const type& input_type) const
    -> caf::expected<std::unique_ptr<aggregation_function>> override {
    auto f = detail::overload{
      [&]<basic_type Type>(
        const Type&) -> caf::expected<std::unique_ptr<aggregation_function>> {
        return std::make_unique<min_max_function<Mode, Type>>(input_type);
      },
      []<complex_type Type>(const Type& type)
        -> caf::expected<std::unique_ptr<aggregation_function>> {
        return caf::make_error(ec::invalid_configuration,
                               fmt::format("max aggregation function does not "
                                           "support complex type {}",
                                           type));
      },
    };
    return caf::visit(f, input_type);
  }

  auto make_aggregation(invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name()).add(expr, "<expr>").parse(inv, ctx));
    return std::make_unique<min_max_instance<Mode>>(std::move(expr));
  }

  auto aggregation_default() const -> data override {
    return caf::none;
  }
};

} // namespace

} // namespace tenzir::plugins::min_max

using namespace tenzir::plugins;

TENZIR_REGISTER_PLUGIN(min_max::plugin<min_max::mode::min>)
TENZIR_REGISTER_PLUGIN(min_max::plugin<min_max::mode::max>)
