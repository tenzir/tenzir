//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/checked_math.hpp"

#include <tenzir/arrow_utils.hpp>
#include <tenzir/concept/parseable/tenzir/time.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/compute/api.h>

namespace tenzir::plugins::duration {

namespace {

class duration_plugin final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "duration";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "string")
          .parse(inv, ctx));
    return function_use::make([expr = std::move(expr)](evaluator eval,
                                                       session ctx) -> series {
      auto b = duration_type::make_arrow_builder(arrow_memory_pool());
      check(b->Reserve(eval.length()));
      for (auto& arg : eval(expr)) {
        const auto f = detail::overload{
          [&](const arrow::NullArray& arg) {
            check(b->AppendNulls(arg.length()));
          },
          [&](const arrow::DurationArray& arg) {
            check(append_array(*b, duration_type{}, arg));
          },
          [&](const arrow::StringArray& arg) {
            for (auto i = 0; i < arg.length(); ++i) {
              if (arg.IsNull(i)) {
                check(b->AppendNull());
                continue;
              }
              auto result = tenzir::duration{};
              constexpr auto p = ignore(*parsers::space) >> parsers::duration
                                 >> ignore(*parsers::space);
              if (p(arg.GetView(i), result)) {
                check(b->Append(result.count()));
                continue;
              }
              diagnostic::warning("failed to parse string")
                .primary(expr)
                .note(fmt::format("tried to convert: {}", arg.GetView(i)))
                .emit(ctx);
              check(b->AppendNull());
            }
          },
          [&](const auto&) {
            diagnostic::warning("`duration` expected `string`, but got `{}`",
                                arg.type.kind())
              .primary(expr)
              .emit(ctx);
            check(b->AppendNulls(arg.length()));
          },
        };
        match(*arg.array, f);
      }
      return series{duration_type{}, finish(*b)};
    });
  }
};

template <class T>
class into_duration_plugin final : public function_plugin {
public:
  into_duration_plugin() = default;

  into_duration_plugin(std::string name) : name_{std::move(name)} {
  }

  auto name() const -> std::string override {
    return name_;
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "number")
          .parse(inv, ctx));
    return function_use::make([this, expr = std::move(expr)](
                                evaluator eval, session ctx) -> series {
      const auto unit = std::chrono::duration_cast<tenzir::duration>(T{1});
      auto b = duration_type::make_arrow_builder(arrow_memory_pool());
      check(b->Reserve(eval.length()));
      for (const auto& arg : eval(expr)) {
        match(
          arg.type,
          [&](const null_type&) {
            check(b->AppendNulls(arg.length()));
          },
          [&](const duration_type& t) {
            diagnostic::warning("interpreting as `{}` has no effect", name())
              .primary(expr, "already has type `duration`")
              .hint("use `count_{}` to extract the number of {}", name(),
                    name())
              .emit(ctx);
            check(append_array_slice(
              *b, t, as<type_to_arrow_array_t<duration_type>>(*arg.array), 0,
              arg.length()));
          },
          [&]<class U>(const U&)
            requires concepts::one_of<U, int64_type, uint64_type, double_type>
          {
            constexpr auto min
              = static_cast<double>(
                  std::numeric_limits<tenzir::duration::rep>::lowest())
                - 1.0;
            constexpr auto max
              = static_cast<double>(
                  std::numeric_limits<tenzir::duration::rep>::max())
                + 1.0;
            auto overflow = false;
            for (auto v :
                 values(U{}, as<type_to_arrow_array_t<U>>(*arg.array))) {
              if (not v) {
                check(b->AppendNull());
                continue;
              }
              if constexpr (not std::same_as<U, double_type>) {
                const auto result = checked_mul(v.value(), unit.count());
                if (not result) {
                  check(b->AppendNull());
                  overflow = true;
                  continue;
                }
                check(b->Append(result.value()));
              } else {
                const auto result
                  = static_cast<double>(v.value()) * unit.count();
                if (not(result > min) || not(result < max)) {
                  check(b->AppendNull());
                  overflow = true;
                  continue;
                }
                check(b->Append(result));
              }
            }
            if (overflow) {
              diagnostic::warning("duration overflow in `{}`", name_)
                .primary(expr)
                .emit(ctx);
            }
          },
          [&](const auto&) {
            diagnostic::warning("`{}` expected `number`, but got `{}`", name_,
                                arg.type.kind())
              .primary(expr)
              .emit(ctx);
            check(b->AppendNulls(arg.length()));
          });
      }
      return series{duration_type{}, finish(*b)};
    });
  }

private:
  std::string name_;
};

template <class T>
class count_duration_plugin final : public function_plugin {
public:
  count_duration_plugin() = default;

  count_duration_plugin(std::string name) : name_{std::move(name)} {
  }

  auto name() const -> std::string override {
    return name_;
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "duration")
          .parse(inv, ctx));
    return function_use::make([this, expr = std::move(expr)](
                                evaluator eval, session ctx) -> series {
      const auto unit = std::chrono::duration_cast<tenzir::duration>(T{1});
      auto b = std::invoke([] {
        if constexpr (std::same_as<T, std::chrono::nanoseconds>) {
          return int64_type::make_arrow_builder(arrow_memory_pool());
        } else {
          return double_type::make_arrow_builder(arrow_memory_pool());
        }
      });
      check(b->Reserve(eval.length()));
      for (auto& arg : eval(expr)) {
        match(
          *arg.array,
          [&](const arrow::NullArray& arg) {
            check(b->AppendNulls(arg.length()));
          },
          [&](const arrow::DurationArray& arg) {
            for (auto v : values(duration_type{}, arg)) {
              if (not v) {
                check(b->AppendNull());
                continue;
              }
              if constexpr (std::same_as<T, std::chrono::nanoseconds>) {
                static_assert(std::same_as<int64_t, tenzir::duration::rep>);
                check(b->Append(v->count()));
              } else {
                check(
                  b->Append(static_cast<double>(v->count()) / unit.count()));
              }
            }
          },
          [&](const auto&) {
            diagnostic::warning("`{}` expected `duration`, but got `{}`", name_,
                                arg.type.kind())
              .primary(expr)
              .emit(ctx);
            check(b->AppendNulls(arg.length()));
          });
      }
      if constexpr (std::same_as<T, std::chrono::nanoseconds>) {
        return series{int64_type{}, finish(*b)};
      }
      return series{double_type{}, finish(*b)};
    });
  }

private:
  std::string name_;
};

} // namespace

} // namespace tenzir::plugins::duration

template <class T>
using count = tenzir::plugins::duration::count_duration_plugin<T>;

template <class T>
using into = tenzir::plugins::duration::into_duration_plugin<T>;

TENZIR_REGISTER_PLUGIN(tenzir::plugins::duration::duration_plugin)
TENZIR_REGISTER_PLUGIN(into<std::chrono::nanoseconds>{"nanoseconds"})
TENZIR_REGISTER_PLUGIN(into<std::chrono::microseconds>{"microseconds"})
TENZIR_REGISTER_PLUGIN(into<std::chrono::milliseconds>{"milliseconds"})
TENZIR_REGISTER_PLUGIN(into<std::chrono::seconds>{"seconds"})
TENZIR_REGISTER_PLUGIN(into<std::chrono::minutes>{"minutes"})
TENZIR_REGISTER_PLUGIN(into<std::chrono::hours>{"hours"})
TENZIR_REGISTER_PLUGIN(into<std::chrono::days>{"days"})
TENZIR_REGISTER_PLUGIN(into<std::chrono::weeks>{"weeks"})
TENZIR_REGISTER_PLUGIN(into<std::chrono::months>{"months"})
TENZIR_REGISTER_PLUGIN(into<std::chrono::years>{"years"})

TENZIR_REGISTER_PLUGIN(count<std::chrono::nanoseconds>{"count_nanoseconds"})
TENZIR_REGISTER_PLUGIN(count<std::chrono::microseconds>{"count_microseconds"})
TENZIR_REGISTER_PLUGIN(count<std::chrono::milliseconds>{"count_milliseconds"})
TENZIR_REGISTER_PLUGIN(count<std::chrono::seconds>{"count_seconds"})
TENZIR_REGISTER_PLUGIN(count<std::chrono::minutes>{"count_minutes"})
TENZIR_REGISTER_PLUGIN(count<std::chrono::hours>{"count_hours"})
TENZIR_REGISTER_PLUGIN(count<std::chrono::days>{"count_days"})
TENZIR_REGISTER_PLUGIN(count<std::chrono::weeks>{"count_weeks"})
TENZIR_REGISTER_PLUGIN(count<std::chrono::months>{"count_months"})
TENZIR_REGISTER_PLUGIN(count<std::chrono::years>{"count_years"})
