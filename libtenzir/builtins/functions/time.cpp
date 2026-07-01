//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_memory_pool.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/concept/parseable/tenzir/time.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/compute/api.h>

#include <cerrno>
#include <chrono>
#include <ctime>
#include <optional>
#include <string_view>

namespace tenzir::plugins::time_ {

// TODO: gcc emits a bogus -Wunused-function warning for this macro when used
// inside an anonymous namespace.
TENZIR_ENUM(ymd_subtype, year, month, day);
TENZIR_ENUM(hms_subtype, hour, minute, second);

namespace {

struct date_fields {
  bool year = false;
  bool month = false;
  bool day = false;
  bool ordinal_day = false;
  bool week_number = false;
  bool iso_week_number = false;
  bool weekday = false;
};

auto mark_date_field(date_fields& fields, char specifier) {
  switch (specifier) {
    default:
      break;
    case 'Y':
    case 'y':
    case 'C':
    case 'G':
    case 'g':
      fields.year = true;
      break;
    case 'b':
    case 'B':
    case 'h':
    case 'm':
      fields.month = true;
      break;
    case 'd':
    case 'e':
      fields.day = true;
      break;
    case 'a':
    case 'A':
    case 'u':
    case 'w':
      fields.weekday = true;
      break;
    case 'U':
    case 'W':
      fields.week_number = true;
      break;
    case 'V':
      fields.week_number = true;
      fields.iso_week_number = true;
      break;
    case 'c':
    case 'D':
    case 'F':
    case 's':
    case 'x':
      fields.year = true;
      fields.month = true;
      fields.day = true;
      break;
    case 'j':
      fields.ordinal_day = true;
      fields.month = true;
      fields.day = true;
      break;
  }
}

auto parse_date_fields(std::string_view format) -> date_fields {
  auto result = date_fields{};
  for (auto i = size_t{0}; i < format.size(); ++i) {
    if (format[i] != '%' or ++i == format.size()) {
      continue;
    }
    if (format[i] == '%') {
      continue;
    }
    while (format[i] == '-' or format[i] == '_' or format[i] == '0'
           or format[i] == '^' or format[i] == '#'
           or (format[i] >= '0' and format[i] <= '9')) {
      if (++i == format.size()) {
        break;
      }
    }
    if (i == format.size()) {
      break;
    }
    if ((format[i] == 'E' or format[i] == 'O') and i + 1 < format.size()) {
      ++i;
    }
    mark_date_field(result, format[i]);
  }
  if (result.week_number and result.weekday) {
    result.month = true;
    result.day = true;
  }
  return result;
}

auto fields_match(const std::tm& lhs, const std::tm& rhs) -> bool {
  return lhs.tm_mon == rhs.tm_mon and lhs.tm_mday == rhs.tm_mday
         and lhs.tm_hour == rhs.tm_hour and lhs.tm_min == rhs.tm_min
         and lhs.tm_sec == rhs.tm_sec;
}

auto as_tm(time value) -> std::optional<std::tm> {
  auto reference_time = std::chrono::system_clock::to_time_t(
    std::chrono::time_point_cast<std::chrono::system_clock::duration>(value));
  auto reference_tm = std::tm{};
  if (gmtime_r(&reference_time, &reference_tm) == nullptr) {
    return std::nullopt;
  }
  return reference_tm;
}

auto apply_ordinal_day(std::tm& tm) -> bool {
  const auto ordinal_day = tm.tm_yday;
  auto candidate = tm;
  candidate.tm_mon = 0;
  candidate.tm_mday = ordinal_day + 1;
  errno = 0;
  const auto parsed = timegm(&candidate);
  if (parsed == -1 and errno != 0) {
    return false;
  }
  auto normalized = std::tm{};
  if (gmtime_r(&parsed, &normalized) == nullptr) {
    return false;
  }
  if (normalized.tm_year != tm.tm_year or normalized.tm_yday != ordinal_day
      or normalized.tm_hour != tm.tm_hour or normalized.tm_min != tm.tm_min
      or normalized.tm_sec != tm.tm_sec) {
    return false;
  }
  tm.tm_mon = normalized.tm_mon;
  tm.tm_mday = normalized.tm_mday;
  return true;
}

auto resolve_missing_year(std::tm& tm, time reference, long offset) -> bool {
  const auto reference_tm = as_tm(reference);
  if (not reference_tm) {
    return false;
  }
  const auto reference_year = reference_tm->tm_year + 1900;
  auto best_year = std::optional<int>{};
  auto best_delta = std::optional<duration>{};
  auto search = [&](int radius) {
    best_year = std::nullopt;
    best_delta = std::nullopt;
    for (auto year = reference_year - radius; year <= reference_year + radius;
         ++year) {
      auto candidate = tm;
      candidate.tm_year = year - 1900;
      const auto original = candidate;
      errno = 0;
      const auto parsed = timegm(&candidate);
      if (parsed == -1 and errno != 0) {
        continue;
      }
      auto normalized = std::tm{};
      if (gmtime_r(&parsed, &normalized) == nullptr) {
        continue;
      }
      if (not fields_match(original, normalized)) {
        continue;
      }
      const auto candidate_time = time::clock::from_time_t(parsed - offset);
      const auto delta = candidate_time > reference
                           ? candidate_time - reference
                           : reference - candidate_time;
      if (not best_delta or delta < *best_delta) {
        best_year = year;
        best_delta = delta;
      }
    }
  };
  search(1);
  if (not best_year) {
    search(4);
  }
  if (not best_year) {
    return false;
  }
  tm.tm_year = *best_year - 1900;
  return true;
}

class time_ final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.time";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
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

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "time")
          .parse(inv, ctx));
    return function_use::make([expr = std::move(expr),
                               this](evaluator eval, session ctx) -> series {
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
              check(
                append_builder(duration_type{}, *b,
                               view_at<time_type>(arg, i)->time_since_epoch()));
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

  auto make_function(function_invocation inv, session ctx) const
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

  auto make_function(function_invocation inv, session ctx) const
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
              auto b = arrow::Int64Builder{tenzir::arrow_memory_pool()};
              check(b.Reserve(arg.length()));
              for (auto i = int64_t{0}; i < arg.length(); ++i) {
                if (arg.IsNull(i)) {
                  check(b.AppendNull());
                  continue;
                }
                auto value = *view_at<time_type>(arg, i);
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

  auto make_function(function_invocation inv, session ctx) const
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
              auto b = arrow::DoubleBuilder{tenzir::arrow_memory_pool()};
              check(b.Reserve(arg.length()));
              for (auto i = int64_t{0}; i < arg.length(); ++i) {
                if (arg.IsNull(i)) {
                  check(b.AppendNull());
                  continue;
                }
                auto value = *view_at<time_type>(arg, i);
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
            auto b = arrow::Int64Builder{tenzir::arrow_memory_pool()};
            check(b.Reserve(arg.length()));
            for (auto i = int64_t{0}; i < arg.length(); ++i) {
              if (arg.IsNull(i)) {
                check(b.AppendNull());
                continue;
              }
              auto value = *view_at<time_type>(arg, i);
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

  auto make_function(function_invocation inv, session ctx) const
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

  auto make_function(function_invocation inv, session ctx) const
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

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto subject_expr = ast::expression{};
    auto format = located<std::string>{};
    auto reference = std::optional<ast::expression>{};
    TRY(argument_parser2::function(name())
          .positional("input", subject_expr, "string")
          .positional("format", format)
          .named("reference", reference, "time")
          .parse(inv, ctx));
    return function_use::make([fn = inv.call.fn.get_location(),
                               subject_expr = std::move(subject_expr),
                               format = std::move(format),
                               reference = std::move(reference)](evaluator eval,
                                                                 session ctx) {
      const auto result_type = time_type{};
      const auto parsed_date_fields = parse_date_fields(format.inner);
      auto null_reference = series::null(null_type{}, eval.length());
      auto references = reference ? eval(*reference)
                                  : multi_series{std::move(null_reference)};
      return map_series(
        eval(subject_expr), std::move(references),
        [&](series subject, series reference_series) {
          return match(
            *subject.array,
            [&](const arrow::StringArray& array) {
              auto error = false;
              auto reference_type_error = false;
              auto reference_ignored = false;
              auto missing_reference = false;
              auto deprecated_missing_reference = false;
              auto unsupported_reference = false;
              auto b = time_type::make_arrow_builder(arrow_memory_pool());
              check(b->Reserve(array.length()));
              auto nul_terminated = std::string{};
              const auto reference_array = reference_series.as<time_type>();
              for (auto i = int64_t{0}; i < array.length(); ++i) {
                if (array.IsNull(i)) {
                  b->UnsafeAppendNull();
                  continue;
                }
                if (reference and parsed_date_fields.iso_week_number) {
                  unsupported_reference = true;
                  b->UnsafeAppendNull();
                  continue;
                }
                const auto str = array.GetView(i);
                auto tm = std::tm{};
                nul_terminated = str;
                // Start from the UNIX epoch. The format scanner below decides
                // which date fields are missing; using extreme sentinels here
                // breaks platform `strptime` implementations for some formats.
                tm.tm_mon = 0;
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
                const auto needs_year = not parsed_date_fields.year;
                const auto needs_month = not parsed_date_fields.month;
                const auto needs_day = not parsed_date_fields.day;
                const auto needs_reference
                  = needs_year or needs_month or needs_day;
                if (reference
                    and ((parsed_date_fields.week_number and needs_year)
                         or (parsed_date_fields.weekday and needs_year
                             and (needs_month or needs_day))
                         or (needs_year and parsed_date_fields.ordinal_day))) {
                  unsupported_reference = true;
                  b->UnsafeAppendNull();
                  continue;
                }
                if (needs_reference) {
                  if (reference_array) {
                    if (not reference_array->array->IsValid(i)) {
                      missing_reference = true;
                      b->UnsafeAppendNull();
                      continue;
                    }
                    const auto ref
                      = *view_at<time_type>(*reference_array->array, i);
                    const auto ref_tm = as_tm(ref);
                    if (not ref_tm) {
                      error = true;
                      b->UnsafeAppendNull();
                      continue;
                    }
                    if (not needs_year or needs_month != needs_day) {
                      unsupported_reference = true;
                      b->UnsafeAppendNull();
                      continue;
                    }
                    if (needs_month) {
                      tm.tm_mon = ref_tm->tm_mon;
                    }
                    if (needs_day) {
                      tm.tm_mday = ref_tm->tm_mday;
                    }
                    if (parsed_date_fields.ordinal_day or needs_month
                        or needs_day) {
                      tm.tm_year = ref_tm->tm_year;
                    } else if (not resolve_missing_year(tm, ref, offset)) {
                      error = true;
                      b->UnsafeAppendNull();
                      continue;
                    }
                  } else if (not reference) {
                    if (needs_year) {
                      deprecated_missing_reference = true;
                      tm.tm_year = 70;
                    }
                    if (needs_month) {
                      tm.tm_mon = 0;
                    }
                    if (needs_day) {
                      tm.tm_mday = 1;
                    }
                  } else if (is<null_type>(reference_series.type)) {
                    missing_reference = true;
                    b->UnsafeAppendNull();
                    continue;
                  } else {
                    reference_type_error = true;
                    b->UnsafeAppendNull();
                    continue;
                  }
                } else if (reference) {
                  reference_ignored = true;
                }
                if (parsed_date_fields.ordinal_day
                    and not apply_ordinal_day(tm)) {
                  error = true;
                  b->UnsafeAppendNull();
                  continue;
                }
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
              if (reference_type_error) {
                diagnostic::warning("`parse_time` expected `reference` to be "
                                    "`time`, but got `{}`",
                                    reference_series.type.kind())
                  .primary(*reference)
                  .emit(ctx);
              }
              if (reference_ignored) {
                diagnostic::warning(
                  "`parse_time` ignores `reference` "
                  "because the format includes a complete date")
                  .primary(*reference)
                  .secondary(format)
                  .emit(ctx);
              }
              if (missing_reference) {
                diagnostic::warning("`parse_time` cannot fill missing date "
                                    "fields because `reference` is null")
                  .primary(*reference)
                  .emit(ctx);
              }
              if (unsupported_reference) {
                diagnostic::warning("`parse_time` cannot fill unsupported date "
                                    "fields from `reference`")
                  .primary(*reference)
                  .secondary(format)
                  .emit(ctx);
              }
              if (deprecated_missing_reference) {
                diagnostic::warning("`parse_time` parsed a timestamp without "
                                    "a year as 1970; this is deprecated and "
                                    "will become an error in a future release")
                  .primary(subject_expr)
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
