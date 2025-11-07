//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_memory_pool.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/concept/parseable/tenzir/time.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/compute/api.h>

#include <chrono>

namespace tenzir::plugins::time_ {

// TODO: gcc emits a bogus -Wunused-function warning for this macro when used
// inside an anonymous namespace.
TENZIR_ENUM(ymd_subtype, year, month, day);
TENZIR_ENUM(hms_subtype, hour, minute, second);

namespace {

class time_ final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.time";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function("time")
          .positional("x", expr, "string")
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr)](evaluator eval, session ctx) -> series {
        auto b = arrow::TimestampBuilder{
          std::make_shared<arrow::TimestampType>(arrow::TimeUnit::NANO),
          arrow_memory_pool()};
        check(b.Reserve(eval.length()));
        for (auto& arg : eval(expr)) {
          auto f = detail::overload{
            [&](const arrow::NullArray& arg) {
              check(b.AppendNulls(arg.length()));
            },
            [&](const arrow::TimestampArray& arg) {
              check(append_array(b, time_type{}, arg));
            },
            [&](const arrow::StringArray& arg) {
              for (auto i = 0; i < arg.length(); ++i) {
                if (arg.IsNull(i)) {
                  check(b.AppendNull());
                  continue;
                }
                auto result = tenzir::time{};
                if (parsers::time(arg.GetView(i), result)) {
                  check(b.Append(result.time_since_epoch().count()));
                } else {
                  // TODO: Warning.
                  check(b.AppendNull());
                }
              }
            },
            [&](const auto&) {
              diagnostic::warning("`time` expected `string`, but got `{}`",
                                  arg.type.kind())
                .primary(expr)
                .emit(ctx);
              check(b.AppendNulls(arg.length()));
            },
          };
          match(*arg.array, f);
        }
        return series{time_type{}, finish(b)};
      });
  }
};

class since_epoch final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "since_epoch";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "time")
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr), this](evaluator eval, session ctx) -> series {
        auto b = duration_type::make_arrow_builder(arrow_memory_pool());
        check(b->Reserve(eval.length()));
        for (auto& arg : eval(expr)) {
          auto f = detail::overload{
            [&](const arrow::NullArray& arg) {
              check(b->AppendNulls(arg.length()));
            },
            [&](const arrow::TimestampArray& arg) {
              auto& ty = as<arrow::TimestampType>(*arg.type());
              TENZIR_ASSERT(ty.timezone().empty());
              for (auto i = 0; i < arg.length(); ++i) {
                if (arg.IsNull(i)) {
                  check(b->AppendNull());
                  continue;
                }
                check(append_builder(
                  duration_type{}, *b,
                  value_at(time_type{}, arg, i).time_since_epoch()));
              }
            },
            [&](const auto&) {
              diagnostic::warning("`{}` expected `time`, but got `{}`", name(),
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

class from_epoch final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "from_epoch";
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
    return function_use::make(
      [expr = std::move(expr), this](evaluator eval, session ctx) {
        return map_series(eval(expr), [&](series arg) {
          const auto f = detail::overload{
            [](const arrow::NullArray& arg) {
              return series::null(time_type{}, arg.length());
            },
            [](const arrow::DurationArray& arg) {
              auto b = time_type::make_arrow_builder(arrow_memory_pool());
              check(b->Reserve(arg.length()));
              for (auto i = int64_t{}; i < arg.length(); ++i) {
                if (arg.IsNull(i)) {
                  check(b->AppendNull());
                  continue;
                }
                check(b->Append(arg.Value(i)));
              }
              return series{time_type{}, finish(*b)};
            },
            [&](const auto&) {
              diagnostic::warning("`{}` expected `duration`, but got `{}`",
                                  name(), arg.type.kind())
                .primary(expr)
                .emit(ctx);
              return series::null(time_type{}, arg.length());
            },
          };
          return match(*arg.array, f);
        });
      });
  }
};

class year_month_day final : public function_plugin {
public:
  explicit year_month_day(ymd_subtype field) : ymd_subtype_(field) {
  }

  auto name() const -> std::string override {
    return std::string{to_string(ymd_subtype_)};
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "time")
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr), this](evaluator eval, session ctx) {
        return map_series(eval(expr), [&](series arg) {
          auto f = detail::overload{
            [](const arrow::NullArray& arg) {
              return series::null(int64_type{}, arg.length());
            },
            [&](const arrow::TimestampArray& arg) {
              auto& ty = as<arrow::TimestampType>(*arg.type());
              TENZIR_ASSERT(ty.timezone().empty());
              auto b = arrow::Int64Builder{};
              check(b.Reserve(arg.length()));
              for (auto i = int64_t{0}; i < arg.length(); ++i) {
                if (arg.IsNull(i)) {
                  check(b.AppendNull());
                  continue;
                }
                auto&& value = value_at(time_type{}, arg, i);
                const std::chrono::year_month_day ymd{
                  std::chrono::floor<std::chrono::days>(value)};
                auto result = int64_t{0};
                switch (ymd_subtype_) {
                  case ymd_subtype::year:
                    result = static_cast<int>(ymd.year());
                    break;
                  case ymd_subtype::month:
                    result = static_cast<unsigned>(ymd.month());
                    break;
                  case ymd_subtype::day:
                    result = static_cast<unsigned>(ymd.day());
                    break;
                }
                check(append_builder(int64_type{}, b, result));
              }
              return series{int64_type{}, finish(b)};
            },
            [&](const auto&) {
              diagnostic::warning("`{}` expected `time`, but got `{}`", name(),
                                  arg.type.kind())
                .primary(expr)
                .emit(ctx);
              return series::null(int64_type{}, arg.length());
            },
          };
          return match(*arg.array, f);
        });
      });
  }

private:
  ymd_subtype ymd_subtype_;
};

class hour_minute_second final : public function_plugin {
public:
  explicit hour_minute_second(hms_subtype field) : hms_subtype_(field) {
  }

  auto name() const -> std::string override {
    return std::string{to_string(hms_subtype_)};
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "time")
          .parse(inv, ctx));
    return function_use::make([expr = std::move(expr), this](
                                evaluator eval, session ctx) -> multi_series {
      return map_series(eval(expr), [&](series arg) -> series {
        // For seconds, we return a double to include subsecond precision
        auto return_type = hms_subtype_ == hms_subtype::second
                             ? type{double_type{}}
                             : type{int64_type{}};
        auto f = detail::overload{
          [&](const arrow::NullArray& arg) -> series {
            return series::null(return_type, arg.length());
          },
          [&](const arrow::TimestampArray& arg) -> series {
            auto& ty = as<arrow::TimestampType>(*arg.type());
            TENZIR_ASSERT(ty.timezone().empty());
            if (hms_subtype_ == hms_subtype::second) {
              auto b = arrow::DoubleBuilder{};
              check(b.Reserve(arg.length()));
              for (auto i = int64_t{0}; i < arg.length(); ++i) {
                if (arg.IsNull(i)) {
                  check(b.AppendNull());
                  continue;
                }
                auto&& value = value_at(time_type{}, arg, i);
                auto duration_since_day_start
                  = value - std::chrono::floor<std::chrono::days>(value);
                auto hours = std::chrono::duration_cast<std::chrono::hours>(
                  duration_since_day_start);
                duration_since_day_start -= hours;
                auto minutes = std::chrono::duration_cast<std::chrono::minutes>(
                  duration_since_day_start);
                duration_since_day_start -= minutes;
                // Convert remaining duration to seconds with fractional part
                auto seconds_double
                  = static_cast<double>(duration_since_day_start.count()) / 1e9;
                check(b.Append(seconds_double));
              }
              return series{double_type{}, finish(b)};
            }
            auto b = arrow::Int64Builder{};
            check(b.Reserve(arg.length()));
            for (auto i = int64_t{0}; i < arg.length(); ++i) {
              if (arg.IsNull(i)) {
                check(b.AppendNull());
                continue;
              }
              auto&& value = value_at(time_type{}, arg, i);
              const auto duration_since_day_start
                = value - std::chrono::floor<std::chrono::days>(value);
              const auto hours = std::chrono::duration_cast<std::chrono::hours>(
                duration_since_day_start);
              if (hms_subtype_ == hms_subtype::hour) {
                check(b.Append(hours.count()));
                continue;
              }
              TENZIR_ASSERT(hms_subtype_ == hms_subtype::minute);
              const auto minutes
                = std::chrono::duration_cast<std::chrono::minutes>(
                  duration_since_day_start - hours);
              check(b.Append(minutes.count()));
            }
            return series{int64_type{}, finish(b)};
          },
          [&](const auto&) -> series {
            diagnostic::warning("`{}` expected `time`, but got `{}`", name(),
                                arg.type.kind())
              .primary(expr)
              .emit(ctx);
            return series::null(return_type, arg.length());
          },
        };
        return match(*arg.array, f);
      });
    });
  }

private:
  hms_subtype hms_subtype_;
};

class now final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.now";
  }

  auto is_deterministic() const -> bool override {
    return false;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    TRY(argument_parser2::function("now").parse(inv, ctx));
    return function_use::make([](evaluator eval, session ctx) -> series {
      TENZIR_UNUSED(ctx);
      auto result = time{time::clock::now()};
      auto b = series_builder{type{time_type{}}};
      for (auto i = int64_t{0}; i < eval.length(); ++i) {
        b.data(result);
      }
      return b.finish_assert_one_array();
    });
  }
};

class format_time : public virtual function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.format_time";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto subject_expr = ast::expression{};
    auto format = located<std::string>{};
    auto locale = std::optional<located<std::string>>{};
    TRY(argument_parser2::function(name())
          .positional("input", subject_expr, "time")
          .positional("format", format)
          .named("locale", locale)
          .parse(inv, ctx));
    return function_use::make(
      [fn = inv.call.fn.get_location(), subject_expr = std::move(subject_expr),
       format = std::move(format),
       locale = std::move(locale)](evaluator eval, session ctx) {
        auto result_type = string_type{};
        auto result_arrow_type
          = std::shared_ptr<arrow::DataType>{result_type.to_arrow_type()};
        return map_series(eval(subject_expr), [&](series subject) {
          return match(
            *subject.array,
            [&](const arrow::TimestampArray& array) {
              auto options = arrow::compute::StrftimeOptions(
                format.inner, locale ? locale->inner : "C");
              auto result
                = arrow::compute::CallFunction("strftime", {array}, &options);
              if (not result.ok()) {
                diagnostic::warning("{}", result.status().ToString())
                  .primary(fn)
                  .emit(ctx);
                return series::null(result_type, subject.length());
              }
              return series{result_type, result.MoveValueUnsafe().make_array()};
            },
            [&](const arrow::NullArray& array) {
              return series::null(result_type, array.length());
            },
            [&](const auto&) {
              diagnostic::warning("`format_time` expected `time`, but got `{}`",
                                  subject.type.kind())
                .primary(subject_expr)
                .emit(ctx);
              return series::null(result_type, subject.length());
            });
        });
      });
  }
};

class parse_time : public virtual function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.parse_time";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto subject_expr = ast::expression{};
    auto format = located<std::string>{};
    TRY(argument_parser2::function(name())
          .positional("input", subject_expr, "string")
          .positional("format", format)
          .parse(inv, ctx));
    return function_use::make(
      [fn = inv.call.fn.get_location(), subject_expr = std::move(subject_expr),
       format = std::move(format)](evaluator eval, session ctx) {
        const auto result_type = time_type{};
        const auto cast_to = arrow::timestamp(arrow::TimeUnit::NANO);
        return map_series(eval(subject_expr), [&](series subject) {
          return match(
            *subject.array,
            [&](const arrow::StringArray& array) {
              auto error = false;
              auto b = time_type::make_arrow_builder(arrow_memory_pool());
              check(b->Reserve(array.length()));
              auto nul_terminated = std::string{};
              for (const auto& str : array) {
                if (not str) {
                  b->UnsafeAppendNull();
                  continue;
                }
                auto tm = std::tm{};
                nul_terminated = str.value();
                // Set the default day and year to match UNIX epoch.
                tm.tm_mday = 1;
                tm.tm_year = 70;
                tm.tm_isdst = -1;
                auto res
                  = strptime(nul_terminated.c_str(), format.inner.c_str(), &tm);
                if (res != nul_terminated.c_str() + nul_terminated.length()) {
                  error = true;
                  b->UnsafeAppendNull();
                  continue;
                }
                const auto offset = tm.tm_gmtoff;
                errno = 0;
                auto parsed = timegm(&tm);
                if (parsed == -1 and errno != 0) {
                  error = true;
                  b->UnsafeAppendNull();
                  continue;
                }
                parsed -= offset;
                const auto tp = time_point_cast<std::chrono::nanoseconds>(
                  time::clock::from_time_t(parsed));
                b->UnsafeAppend(tp.time_since_epoch().count());
              }
              if (error) {
                diagnostic::warning("failed to parse timestamp")
                  .primary(subject_expr)
                  .secondary(format)
                  .emit(ctx);
              }
              return series{time_type{}, finish(*b)};
            },
            [&](const arrow::NullArray& array) {
              return series::null(result_type, array.length());
            },
            [&](const auto&) {
              diagnostic::warning("`parse_time` expected `string`, but got "
                                  "`{}`",
                                  subject.type.kind())
                .primary(subject_expr)
                .emit(ctx);
              return series::null(result_type, subject.length());
            });
        });
      });
  }
};
} // namespace

} // namespace tenzir::plugins::time_

using namespace tenzir::plugins::time_;

TENZIR_REGISTER_PLUGIN(time_)
TENZIR_REGISTER_PLUGIN(since_epoch)
TENZIR_REGISTER_PLUGIN(from_epoch)
TENZIR_REGISTER_PLUGIN(year_month_day{ymd_subtype::year});
TENZIR_REGISTER_PLUGIN(year_month_day{ymd_subtype::month});
TENZIR_REGISTER_PLUGIN(year_month_day{ymd_subtype::day});
TENZIR_REGISTER_PLUGIN(hour_minute_second{hms_subtype::hour});
TENZIR_REGISTER_PLUGIN(hour_minute_second{hms_subtype::minute});
TENZIR_REGISTER_PLUGIN(hour_minute_second{hms_subtype::second});
TENZIR_REGISTER_PLUGIN(now)
TENZIR_REGISTER_PLUGIN(format_time)
TENZIR_REGISTER_PLUGIN(parse_time)
