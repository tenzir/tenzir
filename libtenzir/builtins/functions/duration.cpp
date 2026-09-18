//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/checked_math.hpp"
#include "tenzir/nova/eval.hpp"
#include "tenzir/nova/eval_kernel.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/nova/function_plugin.hpp"
#include "tenzir/nova/type_system.hpp"

#include <tenzir/arrow_utils.hpp>
#include <tenzir/concept/parseable/tenzir/time.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/compute/api.h>

#include <string_view>

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

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "string")
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr)](evaluator eval, session ctx) -> series {
        auto b = duration_type::make_arrow_builder(arrow_memory_pool());
        check(b->Reserve(eval.length()));
        auto failed = Option<std::string>{};
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
                if (not failed) {
                  failed = std::string{arg.GetView(i)};
                }
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
        if (failed) {
          diagnostic::warning("failed to parse string")
            .primary(expr)
            .note(fmt::format("tried to convert: {}", *failed))
            .emit(ctx);
        }
        return series{duration_type{}, finish(*b)};
      });
  }
};

template <class T, bool Count = false>
constexpr auto duration_function_name() -> std::string_view {
  if constexpr (std::same_as<T, std::chrono::nanoseconds>) {
    return Count ? "count_nanoseconds" : "nanoseconds";
  } else if constexpr (std::same_as<T, std::chrono::microseconds>) {
    return Count ? "count_microseconds" : "microseconds";
  } else if constexpr (std::same_as<T, std::chrono::milliseconds>) {
    return Count ? "count_milliseconds" : "milliseconds";
  } else if constexpr (std::same_as<T, std::chrono::seconds>) {
    return Count ? "count_seconds" : "seconds";
  } else if constexpr (std::same_as<T, std::chrono::minutes>) {
    return Count ? "count_minutes" : "minutes";
  } else if constexpr (std::same_as<T, std::chrono::hours>) {
    return Count ? "count_hours" : "hours";
  } else if constexpr (std::same_as<T, std::chrono::days>) {
    return Count ? "count_days" : "days";
  } else if constexpr (std::same_as<T, std::chrono::weeks>) {
    return Count ? "count_weeks" : "weeks";
  } else if constexpr (std::same_as<T, std::chrono::months>) {
    return Count ? "count_months" : "months";
  } else if constexpr (std::same_as<T, std::chrono::years>) {
    return Count ? "count_years" : "years";
  } else {
    static_assert(std::same_as<T, void>, "unsupported duration unit");
  }
}

struct IntoDurationArgs {
  nova::ValueArgument x;
  location call;
};

template <class T>
class IntoDurationFunction final {
public:
  auto eval(IntoDurationArgs const& args, nova::EvalFrame frame) const
    -> nova::Array<nova::Data> {
    auto const name = duration_function_name<T>();
    const auto unit = std::chrono::duration_cast<tenzir::duration>(T{1});
    constexpr auto min = static_cast<double>(
                           std::numeric_limits<tenzir::duration::rep>::lowest())
                         - 1.0;
    constexpr auto max
      = static_cast<double>(std::numeric_limits<tenzir::duration::rep>::max())
        + 1.0;
    auto warn_no_effect = nova::WarnOnce{};
    auto warn_overflow = nova::WarnOnce{};
    return nova::apply_kernel<1>(
      frame, name, {args.x}, args.call,
      detail::overload{
        [&](diagnostic_handler& dh,
            nova::Duration v) -> Option<nova::Duration> {
          warn_no_effect(
            dh,
            diagnostic::warning("interpreting as `{}` has no effect", name)
              .primary(args.x.source, "already has type `duration`")
              .hint("use `count_{}` to extract the number of {}", name, name));
          return v;
        },
        [&](diagnostic_handler& dh, nova::Int v) -> Option<nova::Duration> {
          const auto result = checked_mul(v, unit.count());
          if (not result) {
            warn_overflow(dh,
                          diagnostic::warning("duration overflow in `{}`", name)
                            .primary(args.x.source));
            return None{};
          }
          return nova::Duration{*result};
        },
        [&](diagnostic_handler& dh, nova::UInt v) -> Option<nova::Duration> {
          const auto result = checked_mul(v, unit.count());
          if (not result) {
            warn_overflow(dh,
                          diagnostic::warning("duration overflow in `{}`", name)
                            .primary(args.x.source));
            return None{};
          }
          return nova::Duration{*result};
        },
        [&](diagnostic_handler& dh, nova::Float v) -> Option<nova::Duration> {
          const auto result = static_cast<double>(v) * unit.count();
          if (not(result > min) or not(result < max)) {
            warn_overflow(dh,
                          diagnostic::warning("duration overflow in `{}`", name)
                            .primary(args.x.source));
            return None{};
          }
          return nova::Duration{static_cast<tenzir::duration::rep>(result)};
        },
      });
  }
};

template <class T>
class into_duration_plugin final : public nova::FunctionPlugin {
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

  auto describe() const -> nova::FunctionDescription override {
    auto d
      = nova::FunctionDescriber<IntoDurationArgs, IntoDurationFunction<T>>{};
    d.positional("x", &IntoDurationArgs::x, "number");
    d.call_location(&IntoDurationArgs::call);
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "number")
          .parse(inv, ctx));
    return function_use::make(
      [this, expr = std::move(expr)](evaluator eval, session ctx) -> series {
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
                  check(b->Append(*result));
                } else {
                  const auto result
                    = static_cast<double>(v.value()) * unit.count();
                  if (not(result > min) or not(result < max)) {
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

struct CountDurationArgs {
  nova::ValueArgument x;
  location call;
};

template <class T>
class CountDurationFunction final {
public:
  auto eval(CountDurationArgs const& args, nova::EvalFrame frame) const
    -> nova::Array<nova::Data> {
    auto const name = duration_function_name<T, true>();
    const auto unit = std::chrono::duration_cast<tenzir::duration>(T{1});
    if constexpr (std::same_as<T, std::chrono::nanoseconds>) {
      static_assert(std::same_as<int64_t, tenzir::duration::rep>);
      return nova::apply_kernel<1>(frame, name, {args.x}, args.call,
                                   [](diagnostic_handler&,
                                      nova::Duration v) -> Option<nova::Int> {
                                     return v.count();
                                   });
    } else {
      return nova::apply_kernel<1>(
        frame, name, {args.x}, args.call,
        [unit](diagnostic_handler&, nova::Duration v) -> Option<nova::Float> {
          return static_cast<double>(v.count()) / unit.count();
        });
    }
  }
};

template <class T>
class count_duration_plugin final : public nova::FunctionPlugin {
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

  auto describe() const -> nova::FunctionDescription override {
    auto d
      = nova::FunctionDescriber<CountDurationArgs, CountDurationFunction<T>>{};
    d.positional("x", &CountDurationArgs::x, "duration");
    d.call_location(&CountDurationArgs::call);
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "duration")
          .parse(inv, ctx));
    return function_use::make(
      [this, expr = std::move(expr)](evaluator eval, session ctx) -> series {
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
              diagnostic::warning("`{}` expected `duration`, but got `{}`",
                                  name_, arg.type.kind())
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
