//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/array_builder.hpp"
#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/eval.hpp"
#include "tenzir/nova/eval_kernel.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/nova/function_plugin.hpp"
#include "tenzir/nova/type_system.hpp"
#include "tenzir/option.hpp"

#include <tenzir/arrow_memory_pool.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/concept/parseable/tenzir/time.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/time_zone.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/compute/api.h>
#include <arrow/vendored/datetime/date.h>

#include <cerrno>
#include <chrono>
#include <ctime>
#include <locale>
#include <map>
#include <ostream>
#include <stdexcept>
#include <streambuf>
#include <string>
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
  bool complete_calendar_date = false;
  /// Whether the format pins the instant itself through a UTC offset or
  /// seconds since the epoch. `%Z` does not count: `strptime` consumes a zone
  /// name without resolving it, and abbreviations are ambiguous.
  bool absolute = false;
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
    case 'x':
      fields.year = true;
      fields.month = true;
      fields.day = true;
      break;
    case 's':
      fields.year = true;
      fields.month = true;
      fields.day = true;
      fields.absolute = true;
      break;
    case 'z':
      fields.absolute = true;
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
  // Record calendar completeness before week-based inference: a date derived
  // from week number and weekday is not a calendar date.
  result.complete_calendar_date = result.year and result.month and result.day;
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

auto as_tm(time value) -> Option<std::tm> {
  auto reference_time = std::chrono::system_clock::to_time_t(
    std::chrono::time_point_cast<std::chrono::system_clock::duration>(value));
  auto reference_tm = std::tm{};
  if (gmtime_r(&reference_time, &reference_tm) == nullptr) {
    return None{};
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

/// Maps a broken-down time that `timegm` read as UTC to the actual UTC instant.
/// Without a zone, the wall-clock time is UTC shifted by the fixed `offset`
/// that `%z` parsed. With a zone, `TimeZone::to_utc` resolves it, including
/// wall-clock times that a daylight saving transition repeats or skips.
class WallClock {
public:
  WallClock(long offset, TimeZone const* zone) : offset_{offset}, zone_{zone} {
  }

  auto to_utc(std::time_t local) const -> std::time_t {
    if (not zone_) {
      return local - offset_;
    }
    auto const utc = zone_->to_utc(local_time{std::chrono::seconds{local}});
    return std::chrono::duration_cast<std::chrono::seconds>(
             utc.time_since_epoch())
      .count();
  }

private:
  long offset_;
  TimeZone const* zone_;
};

auto resolve_missing_year(std::tm& tm, time reference, WallClock clock)
  -> bool {
  const auto reference_tm = as_tm(reference);
  if (not reference_tm) {
    return false;
  }
  const auto reference_year = reference_tm->tm_year + 1900;
  auto best_year = Option<int>{};
  auto best_delta = Option<duration>{};
  auto search = [&](int radius) {
    best_year = None{};
    best_delta = None{};
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
      const auto candidate_time
        = time::clock::from_time_t(clock.to_utc(parsed));
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

auto resolve_missing_date(std::tm& tm, const std::tm& reference_tm,
                          time reference, WallClock clock) -> bool {
  auto best = Option<std::tm>{};
  auto best_delta = Option<duration>{};
  // Try the reference date and its neighbors so that times of day shortly
  // before or after the reference resolve to the closest instant instead of
  // up to a day away. `timegm` normalizes day 0 and day 32 across month and
  // year boundaries.
  for (auto day_shift = -1; day_shift <= 1; ++day_shift) {
    auto candidate = tm;
    candidate.tm_year = reference_tm.tm_year;
    candidate.tm_mon = reference_tm.tm_mon;
    candidate.tm_mday = reference_tm.tm_mday + day_shift;
    errno = 0;
    const auto parsed = timegm(&candidate);
    if (parsed == -1 and errno != 0) {
      continue;
    }
    const auto candidate_time = time::clock::from_time_t(clock.to_utc(parsed));
    const auto delta = candidate_time > reference ? candidate_time - reference
                                                  : reference - candidate_time;
    if (not best_delta or delta < *best_delta) {
      best = candidate;
      best_delta = delta;
    }
  }
  if (not best) {
    return false;
  }
  tm.tm_year = best->tm_year;
  tm.tm_mon = best->tm_mon;
  tm.tm_mday = best->tm_mday;
  return true;
}

struct TimeArgs {
  nova::ValueArgument x;
  location call;
};

class TimeFunction final {
public:
  static auto eval(TimeArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    auto warn_parse = nova::WarnOnce{};
    return nova::apply_kernel<1>(
      frame, "time", {args.x}, args.call,
      detail::overload{
        [](diagnostic_handler&, nova::Null) -> Option<nova::Time> {
          return None{};
        },
        [](diagnostic_handler&, nova::Time value) -> Option<nova::Time> {
          return value;
        },
        [&](diagnostic_handler& dh,
            std::string_view input) -> Option<nova::Time> {
          auto result = time{};
          if (parsers::time(input, result)) {
            return result;
          }
          warn_parse(dh, diagnostic::warning("`time` failed to parse string")
                           .primary(args.x.source)
                           .note("tried to convert: {}", input));
          return None{};
        },
      });
  }
};

class time_ final : public virtual nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return "time";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto is_cheap() const -> bool override {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<TimeArgs, TimeFunction>{};
    d.positional("x", &TimeArgs::x, "string");
    d.call_location(&TimeArgs::call);
    return std::move(d).finish();
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
        auto failed = Option<std::string>{};
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
                  if (not failed) {
                    failed = std::string{arg.GetView(i)};
                  }
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
        if (failed) {
          diagnostic::warning("`time` failed to parse string")
            .primary(expr)
            .note(fmt::format("tried to convert: {}", *failed))
            .emit(ctx);
        }
        return series{time_type{}, finish(b)};
      });
  }
};

struct SinceEpochArgs {
  nova::ValueArgument x;
  location call;
};

class SinceEpochFunction final {
public:
  static auto eval(SinceEpochArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    return nova::apply_kernel<1>(frame, "since_epoch", {args.x}, args.call,
                                 [](diagnostic_handler&,
                                    nova::Time v) -> Option<nova::Duration> {
                                   return v.time_since_epoch();
                                 });
  }
};

class since_epoch final : public nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return "since_epoch";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto is_cheap() const -> bool override {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<SinceEpochArgs, SinceEpochFunction>{};
    d.positional("x", &SinceEpochArgs::x, "time");
    d.call_location(&SinceEpochArgs::call);
    return std::move(d).finish();
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

struct FromEpochArgs {
  nova::ValueArgument x;
  location call;
};

class FromEpochFunction final {
public:
  static auto eval(FromEpochArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    return nova::apply_kernel<1>(frame, "from_epoch", {args.x}, args.call,
                                 [](diagnostic_handler&,
                                    nova::Duration v) -> Option<nova::Time> {
                                   return nova::Time{v};
                                 });
  }
};

class from_epoch final : public nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return "from_epoch";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto is_cheap() const -> bool override {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<FromEpochArgs, FromEpochFunction>{};
    d.positional("x", &FromEpochArgs::x, "duration");
    d.call_location(&FromEpochArgs::call);
    return std::move(d).finish();
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

/// Arguments of the calendar and clock component functions. The component is
/// not an argument; `describe` stores it here for the shared kernel.
struct TimeComponentArgs {
  nova::ValueArgument x;
  std::string_view name;
  Option<ymd_subtype> ymd;
  Option<hms_subtype> hms;
  location call;
};

class TimeComponentFunction final {
public:
  static auto eval(TimeComponentArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    using namespace nova;
    if (args.hms == hms_subtype::second) {
      // Seconds include the subsecond part and are therefore a `float`.
      return apply_kernel<1>(
        frame, args.name, {args.x}, args.call,
        detail::overload{
          [](diagnostic_handler&, Null) -> Option<Float> {
            return None{};
          },
          [](diagnostic_handler&, Time value) -> Option<Float> {
            auto const since_minute
              = value - std::chrono::floor<std::chrono::minutes>(value);
            return static_cast<double>(since_minute.count()) / 1e9;
          },
        });
    }
    return apply_kernel<1>(
      frame, args.name, {args.x}, args.call,
      detail::overload{
        [](diagnostic_handler&, Null) -> Option<Int> {
          return None{};
        },
        [&](diagnostic_handler&, Time value) -> Option<Int> {
          auto const day = std::chrono::floor<std::chrono::days>(value);
          if (args.ymd) {
            auto const ymd = std::chrono::year_month_day{day};
            switch (*args.ymd) {
              case ymd_subtype::year:
                return int64_t{static_cast<int>(ymd.year())};
              case ymd_subtype::month:
                return int64_t{static_cast<unsigned>(ymd.month())};
              case ymd_subtype::day:
                return int64_t{static_cast<unsigned>(ymd.day())};
            }
            TENZIR_UNREACHABLE();
          }
          auto const since_day = value - day;
          auto const hours
            = std::chrono::duration_cast<std::chrono::hours>(since_day);
          if (args.hms == hms_subtype::hour) {
            return int64_t{hours.count()};
          }
          TENZIR_ASSERT(args.hms == hms_subtype::minute);
          return int64_t{
            std::chrono::duration_cast<std::chrono::minutes>(since_day - hours)
              .count()};
        },
      });
  }
};

class year_month_day final : public nova::FunctionPlugin {
public:
  explicit year_month_day(ymd_subtype field) : ymd_subtype_(field) {
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d
      = nova::FunctionDescriber<TimeComponentArgs, TimeComponentFunction>{};
    d.positional("x", &TimeComponentArgs::x, "time");
    d.call_location(&TimeComponentArgs::call);
    d.validate([field = ymd_subtype_](TimeComponentArgs& args,
                                      diagnostic_handler&) -> failure_or<void> {
      args.name = to_string(field);
      args.ymd = field;
      return {};
    });
    return std::move(d).finish();
  }

  auto name() const -> std::string override {
    return std::string{to_string(ymd_subtype_)};
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto is_cheap() const -> bool override {
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

class hour_minute_second final : public nova::FunctionPlugin {
public:
  explicit hour_minute_second(hms_subtype field) : hms_subtype_(field) {
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d
      = nova::FunctionDescriber<TimeComponentArgs, TimeComponentFunction>{};
    d.positional("x", &TimeComponentArgs::x, "time");
    d.call_location(&TimeComponentArgs::call);
    d.validate([field = hms_subtype_](TimeComponentArgs& args,
                                      diagnostic_handler&) -> failure_or<void> {
      args.name = to_string(field);
      args.hms = field;
      return {};
    });
    return std::move(d).finish();
  }

  auto name() const -> std::string override {
    return std::string{to_string(hms_subtype_)};
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto is_cheap() const -> bool override {
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

struct NowArgs {};

class NowFunction final {
public:
  static auto eval(NowArgs const&, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    return nova::Array<nova::Time>{nova::storage::ConstantStorage<nova::Time>{
      frame.length(), time{time::clock::now()}}};
  }
};

class now final : public nova::FunctionPlugin {
public:
  auto describe() const -> nova::FunctionDescription override {
    return nova::FunctionDescriber<NowArgs, NowFunction>{}.finish();
  }

  auto name() const -> std::string override {
    return "now";
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

struct FormatTimeArgs {
  nova::ValueArgument x;
  located<std::string> format;
  Option<located<std::string>> locale;
  location call;
};

class FormatTimeFunction final {
public:
  static auto eval(FormatTimeArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    using namespace nova;
    auto const& mask = frame.mask();
    auto const length = mask.length();
    auto times = args.x.data.get_alternative<Time>();
    auto time_rows
      = times ? mask & times->present : storage::BitMap{length, false};
    auto nulls = args.x.data.get_alternative<Null>();
    auto null_rows
      = nulls ? mask & nulls->present : storage::BitMap{length, false};
    if (mask.and_not(time_rows).and_not(null_rows).any()) {
      diagnostic::warning("`format_time` expected `time`, but got a different "
                          "type")
        .primary(args.x.source)
        .emit(frame);
    }
    if (not time_rows.any()) {
      return frame.null();
    }
    auto const locale_name = args.locale ? args.locale->inner : "C";
    // Like Arrow's `strftime`, which the legacy implementation uses: the
    // `date` library mishandles `%c` outside of the C locale, see
    // https://github.com/HowardHinnant/date/issues/704.
    if (locale_name != "C" and has_locale_date_time(args.format.inner)) {
      diagnostic::warning("`%c` and `%Ec` are not supported with a locale "
                          "other than `C`")
        .primary(args.format)
        .emit(frame);
      return frame.null();
    }
    auto buffer = AppendBuffer{};
    auto stream = std::ostream{&buffer};
    try {
      stream.imbue(std::locale{locale_name});
    } catch (std::runtime_error const&) {
      diagnostic::warning("unknown locale `{}`", locale_name)
        .primary(*args.locale)
        .emit(frame);
      return frame.null();
    }
    auto ranges
      = storage::DataOwner<storage::Span[]>::make_uninitialized(length);
    for (auto row = storage::Index{0}; row < length; ++row) {
      auto const begin = buffer.data.size();
      if (time_rows.get(row)) {
        // The same formatting as Arrow's `strftime` for a time in UTC.
        arrow_vendored::date::to_stream(stream, args.format.inner.c_str(),
                                        *times->data.get(row));
      }
      ranges.emplace_back(begin, buffer.data.size());
    }
    auto result = Array<String>{
      storage::DenseStringOffsetStorage{buffer.data.finish(), ranges.finish()}};
    return Array<Data>{std::move(result)}.null_where(mask.and_not(time_rows));
  }

private:
  /// Returns whether `format` holds a `%c` or `%Ec` conversion, skipping
  /// `%%` escapes like the `date` library does.
  static auto has_locale_date_time(std::string_view format) -> bool {
    for (auto i = size_t{0}; i + 1 < format.size(); ++i) {
      if (format[i] != '%') {
        continue;
      }
      auto const modifier = format[++i];
      if ((modifier == 'E' or modifier == 'O') and i + 1 < format.size()) {
        ++i;
      }
      if (format[i] == 'c' and modifier != 'O') {
        return true;
      }
    }
    return false;
  }

  /// A stream buffer that appends to a Nova string buffer.
  struct AppendBuffer final : std::streambuf {
    nova::storage::DataOwner<char[]>::Builder data;

    auto overflow(int_type c) -> int_type override {
      if (not traits_type::eq_int_type(c, traits_type::eof())) {
        data.emplace_back(traits_type::to_char_type(c));
      }
      return traits_type::not_eof(c);
    }

    auto xsputn(char const* s, std::streamsize n) -> std::streamsize override {
      data.move_append(s, s + n);
      return n;
    }
  };
};

class format_time : public virtual nova::FunctionPlugin {
public:
  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<FormatTimeArgs, FormatTimeFunction>{};
    d.positional("input", &FormatTimeArgs::x, "time");
    d.positional("format", &FormatTimeArgs::format);
    d.named("locale", &FormatTimeArgs::locale);
    d.call_location(&FormatTimeArgs::call);
    return std::move(d).finish();
  }

  auto name() const -> std::string override {
    return "format_time";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto subject_expr = ast::expression{};
    auto format = located<std::string>{};
    auto locale = Option<located<std::string>>{};
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

/// The `reference` of `parse_time` for one input row.
struct ParseTimeReference {
  enum class Kind {
    /// The call has no `reference`.
    absent,
    /// The row's reference is `value`.
    value,
    /// The row's reference is null.
    null,
    /// The row's reference is not a `time`.
    mistyped,
  };

  Kind kind = Kind::absent;
  time value = {};
};

/// The conditions that `parse_time` reports once per batch.
struct ParseTimeIssues {
  bool error = false;
  bool reference_type_error = false;
  bool reference_ignored = false;
  bool missing_reference = false;
  bool unsupported_reference = false;
  bool deprecated_missing_reference = false;
  bool timezone_ignored = false;
  /// The type of the first `timezone` that is not a `string`.
  Option<std::string> timezone_type_error;
  /// The first `timezone` that names no known time zone.
  Option<std::string> unknown_timezone;
};

/// Looks up the `timezone` of `parse_time`, remembering earlier lookups so that
/// a batch with few distinct zones resolves each of them only once.
class ParseTimeZones {
public:
  /// Returns the zone named `name`, or a null pointer if there is none.
  auto locate(std::string_view name) -> TimeZone const* {
    auto it = cache_.find(name);
    if (it == cache_.end()) {
      auto const zone = TimeZone::locate(name);
      it = cache_.emplace(std::string{name}, zone ? &*zone : nullptr).first;
    }
    return it->second;
  }

  /// Resolves the `timezone` of one row, recording problems in `issues`.
  /// Returns `None` if the row must become null, and a null pointer if the
  /// wall-clock time is UTC.
  auto resolve(Option<std::string_view> name, bool absolute,
               ParseTimeIssues& issues) -> Option<TimeZone const*> {
    using Zone = TimeZone const*;
    if (not name) {
      return Zone{nullptr};
    }
    if (absolute) {
      issues.timezone_ignored = true;
      return Zone{nullptr};
    }
    auto const* zone = locate(*name);
    if (not zone) {
      if (not issues.unknown_timezone) {
        issues.unknown_timezone = std::string{*name};
      }
      return None{};
    }
    return zone;
  }

private:
  std::map<std::string, TimeZone const*, std::less<>> cache_;
};

/// Parses one input string of `parse_time`, filling the date fields that
/// `format` lacks from `reference`. The parsed wall-clock time is in `zone`, or
/// in UTC if `zone` is null; an explicit offset in the input takes precedence.
/// Returns `None` for a row that becomes null and records why in `issues`.
/// `buffer` is scratch space for the NUL-terminated copy that `strptime` needs.
auto parse_time_row(std::string_view input, std::string const& format,
                    date_fields const& fields, ParseTimeReference reference,
                    TimeZone const* zone, std::string& buffer,
                    ParseTimeIssues& issues) -> Option<time> {
  using Kind = ParseTimeReference::Kind;
  auto const has_reference = reference.kind != Kind::absent;
  if (has_reference and fields.iso_week_number
      and not fields.complete_calendar_date) {
    issues.unsupported_reference = true;
    return None{};
  }
  auto tm = std::tm{};
  buffer = input;
  // Start from the UNIX epoch. The format scanner decides which date fields
  // are missing; using extreme sentinels here breaks platform `strptime`
  // implementations for some formats.
  tm.tm_mon = 0;
  tm.tm_mday = 1;
  tm.tm_year = 70;
  tm.tm_isdst = -1;
  auto const* end = strptime(buffer.c_str(), format.c_str(), &tm);
  if (end != buffer.c_str() + buffer.length()) {
    issues.error = true;
    return None{};
  }
  auto const clock = WallClock{tm.tm_gmtoff, fields.absolute ? nullptr : zone};
  auto const needs_year = not fields.year;
  auto const needs_month = not fields.month;
  auto const needs_day = not fields.day;
  auto const needs_reference = needs_year or needs_month or needs_day;
  if (has_reference
      and ((fields.week_number and needs_year)
           or (fields.weekday and needs_year and (needs_month or needs_day))
           or (needs_year and fields.ordinal_day))) {
    issues.unsupported_reference = true;
    return None{};
  }
  if (needs_reference) {
    switch (reference.kind) {
      case Kind::value: {
        auto const reference_tm = as_tm(reference.value);
        if (not reference_tm) {
          issues.error = true;
          return None{};
        }
        if (not needs_year or needs_month != needs_day) {
          issues.unsupported_reference = true;
          return None{};
        }
        // The guard above leaves two cases: the format has no date fields at
        // all, or only the year is missing.
        auto const resolved
          = needs_month
              ? resolve_missing_date(tm, *reference_tm, reference.value, clock)
              : resolve_missing_year(tm, reference.value, clock);
        if (not resolved) {
          issues.error = true;
          return None{};
        }
        break;
      }
      case Kind::absent:
        if (needs_year) {
          issues.deprecated_missing_reference = true;
          tm.tm_year = 70;
        }
        if (needs_month) {
          tm.tm_mon = 0;
        }
        if (needs_day) {
          tm.tm_mday = 1;
        }
        break;
      case Kind::null:
        issues.missing_reference = true;
        return None{};
      case Kind::mistyped:
        issues.reference_type_error = true;
        return None{};
    }
  } else if (has_reference) {
    issues.reference_ignored = true;
  }
  if (fields.ordinal_day and not apply_ordinal_day(tm)) {
    issues.error = true;
    return None{};
  }
  errno = 0;
  auto parsed = timegm(&tm);
  if (parsed == -1 and errno != 0) {
    issues.error = true;
    return None{};
  }
  parsed = clock.to_utc(parsed);
  return time_point_cast<std::chrono::nanoseconds>(
    time::clock::from_time_t(parsed));
}

/// Emits the diagnostics for the conditions in `issues`. `reference` and
/// `reference_type` are only read for conditions that involve a reference.
auto emit_parse_time_issues(ParseTimeIssues const& issues, location input,
                            location format, location reference,
                            std::string_view reference_type,
                            location timezone_location, diagnostic_handler& dh)
  -> void {
  if (issues.timezone_type_error) {
    diagnostic::warning("`parse_time` expected `timezone` to be `string`, but "
                        "got `{}`",
                        *issues.timezone_type_error)
      .primary(timezone_location)
      .emit(dh);
  }
  if (issues.unknown_timezone) {
    diagnostic::warning("`parse_time` got unknown time zone `{}`",
                        *issues.unknown_timezone)
      .primary(timezone_location)
      .hint("use an IANA time zone name, such as `Europe/Berlin` or `UTC`")
      .emit(dh);
  }
  if (issues.timezone_ignored) {
    diagnostic::warning("`parse_time` ignores `timezone` because the format "
                        "includes a time zone")
      .primary(timezone_location)
      .secondary(format)
      .note("`%z` and `%s` take precedence over `timezone`")
      .emit(dh);
  }
  if (issues.error) {
    diagnostic::warning("failed to parse timestamp")
      .primary(input)
      .secondary(format)
      .emit(dh);
  }
  if (issues.reference_type_error) {
    diagnostic::warning("`parse_time` expected `reference` to be `time`, but "
                        "got `{}`",
                        reference_type)
      .primary(reference)
      .emit(dh);
  }
  if (issues.reference_ignored) {
    diagnostic::warning("`parse_time` ignores `reference` because the format "
                        "includes a complete date")
      .primary(reference)
      .secondary(format)
      .emit(dh);
  }
  if (issues.missing_reference) {
    diagnostic::warning("`parse_time` cannot fill missing date fields because "
                        "`reference` is null")
      .primary(reference)
      .emit(dh);
  }
  if (issues.unsupported_reference) {
    diagnostic::warning("`parse_time` cannot fill unsupported date fields from "
                        "`reference`")
      .primary(reference)
      .secondary(format)
      .emit(dh);
  }
  if (issues.deprecated_missing_reference) {
    diagnostic::warning("`parse_time` parsed a timestamp without a year as "
                        "1970; this is deprecated and will become an error in "
                        "a future release")
      .primary(input)
      .emit(dh);
  }
}

struct ParseTimeArgs {
  nova::ValueArgument x;
  located<std::string> format;
  Option<nova::ValueArgument> reference;
  Option<nova::ValueArgument> timezone;
  date_fields fields;
  location call;
};

class ParseTimeFunction final {
public:
  static auto eval(ParseTimeArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    using namespace nova;
    using Kind = ParseTimeReference::Kind;
    using Zone = TimeZone const*;
    auto issues = ParseTimeIssues{};
    auto buffer = std::string{};
    auto reference_type = std::string_view{};
    auto input_tag = Option<std::size_t>{};
    // Resolve the time zones first, so that the parsing kernel only sees
    // indices into `zones`: a null index means UTC, and a negative one turns
    // the row null. Rows with a null or non-string input never parse, so they
    // skip the lookup and its diagnostics, as in the legacy evaluator.
    auto lookup = ParseTimeZones{};
    auto zones = std::vector<Zone>{};
    auto zone_indices = frame.null();
    if (args.timezone) {
      auto indices = std::map<Zone, Int>{};
      zone_indices = apply_kernel<2>(
        frame, "parse_time", {args.x, *args.timezone}, args.call,
        [&]<class S, class Z>(diagnostic_handler&, S, Z zone) -> Option<Int> {
          if constexpr (not std::same_as<S, std::string_view>
                        or std::same_as<Z, Null>) {
            return None{};
          } else if constexpr (std::same_as<Z, std::string_view>) {
            auto resolved = lookup.resolve(zone, args.fields.absolute, issues);
            if (not resolved) {
              return Int{-1};
            }
            if (not *resolved) {
              return None{};
            }
            auto [it, inserted]
              = indices.try_emplace(*resolved, static_cast<Int>(zones.size()));
            if (inserted) {
              zones.push_back(*resolved);
            }
            return it->second;
          } else {
            if (args.fields.absolute) {
              issues.timezone_ignored = true;
              return None{};
            }
            if (not issues.timezone_type_error) {
              issues.timezone_type_error = std::string{
                Type<data_type_list::at<KernelViewList::unique_index_of<Z>>>::
                  static_name};
            }
            return Int{-1};
          }
        });
    }
    auto zone_at = [&](Option<Int> index) -> Option<Zone> {
      if (not index) {
        return Zone{nullptr};
      }
      if (*index < 0) {
        return None{};
      }
      return zones[static_cast<std::size_t>(*index)];
    };
    auto parse = [&](std::string_view input, ParseTimeReference reference,
                     Option<Int> zone_index) -> Option<Time> {
      auto zone = zone_at(zone_index);
      if (not zone) {
        return None{};
      }
      return parse_time_row(input, args.format.inner, args.fields, reference,
                            *zone, buffer, issues);
    };
    auto to_index = []<class Z>(Z zone) -> Option<Int> {
      if constexpr (std::same_as<Z, Int>) {
        return zone;
      } else {
        return None{};
      }
    };
    // Without a `reference`, a null placeholder takes its place in the kernel.
    auto reference = args.reference
                       ? *args.reference
                       : ValueArgument{frame.null(), location::unknown};
    auto result = apply_kernel<3>(
      frame, "parse_time",
      {args.x, reference, ValueArgument{zone_indices, location::unknown}},
      args.call,
      detail::overload{
        []<class R, class Z>(diagnostic_handler&, Null, R, Z) -> Option<Time>
          requires(std::same_as<Z, Int> or std::same_as<Z, Null>)
                  {
                    return None{};
                  },
                  // Rejecting the input here rather than through
                  // `apply_kernel` keeps the types of the other arguments out
                  // of the diagnostic.
                  [&]<class S, class R, class Z>(diagnostic_handler&, S, R,
                                                 Z) -> Option<Time>
                    requires(not std::same_as<S, std::string_view>
                             and not std::same_as<S, Null>
                             and (std::same_as<Z, Int> or std::same_as<Z, Null>))
                            {
                              input_tag = KernelViewList::unique_index_of<S>;
                              return None{};
                            },
                            [&]<class R, class Z>(
                              diagnostic_handler&, std::string_view input,
                              R reference, Z zone) -> Option<Time>
                              requires(std::same_as<Z, Int>
                                       or std::same_as<Z, Null>)
        {
          if (not args.reference) {
            return parse(input, {}, to_index(zone));
          }
          if constexpr (std::same_as<R, Time>) {
            return parse(input, {Kind::value, reference}, to_index(zone));
          } else if constexpr (std::same_as<R, Null>) {
            return parse(input, {Kind::null}, to_index(zone));
          } else {
            reference_type
              = Type<data_type_list::at<KernelViewList::unique_index_of<R>>>::
                static_name;
            return parse(input, {Kind::mistyped}, to_index(zone));
          }
        },
        });
    if (input_tag) {
      warn_rejected_kernel_types(frame, "parse_time", args.call,
                                 std::array{*input_tag});
    }
    emit_parse_time_issues(
      issues, args.x.source, args.format.source,
      args.reference ? args.reference->source : location::unknown,
      reference_type, args.timezone ? args.timezone->source : location::unknown,
      frame);
    return result;
  }
};

class parse_time : public virtual nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return "parse_time";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<ParseTimeArgs, ParseTimeFunction>{};
    d.positional("input", &ParseTimeArgs::x, "string");
    d.positional("format", &ParseTimeArgs::format);
    d.named_optional("reference", &ParseTimeArgs::reference, "time");
    d.named_optional("timezone", &ParseTimeArgs::timezone, "string");
    d.call_location(&ParseTimeArgs::call);
    d.validate(
      [](ParseTimeArgs& args, diagnostic_handler&) -> failure_or<void> {
        args.fields = parse_date_fields(args.format.inner);
        return {};
      });
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto subject_expr = ast::expression{};
    auto format = located<std::string>{};
    auto reference = Option<ast::expression>{};
    auto timezone_expr = Option<ast::expression>{};
    TRY(argument_parser2::function(name())
          .positional("input", subject_expr, "string")
          .positional("format", format)
          .named("reference", reference, "time")
          .named("timezone", timezone_expr, "string")
          .parse(inv, ctx));
    return function_use::make(
      [subject_expr = std::move(subject_expr), format = std::move(format),
       reference = std::move(reference),
       timezone_expr = std::move(timezone_expr)](evaluator eval, session ctx) {
        using Kind = ParseTimeReference::Kind;
        const auto result_type = time_type{};
        const auto parsed_date_fields = parse_date_fields(format.inner);
        auto references
          = reference ? eval(*reference)
                      : multi_series{series::null(null_type{}, eval.length())};
        auto timezones
          = timezone_expr
              ? eval(*timezone_expr)
              : multi_series{series::null(null_type{}, eval.length())};
        auto zones = ParseTimeZones{};
        auto arguments = std::array{eval(subject_expr), std::move(references),
                                    std::move(timezones)};
        return map_series(
          arguments, [&](std::span<series> slices) -> multi_series {
            auto const& subject = slices[0];
            auto const& reference_series = slices[1];
            auto const& timezone_series = slices[2];
            return match(
              *subject.array,
              [&](const arrow::StringArray& array) {
                auto issues = ParseTimeIssues{};
                auto b = time_type::make_arrow_builder(arrow_memory_pool());
                check(b->Reserve(array.length()));
                auto buffer = std::string{};
                const auto reference_array = reference_series.as<time_type>();
                const auto timezone_array = timezone_series.as<string_type>();
                for (auto i = int64_t{0}; i < array.length(); ++i) {
                  if (array.IsNull(i)) {
                    b->UnsafeAppendNull();
                    continue;
                  }
                  auto row_reference = ParseTimeReference{};
                  if (not reference) {
                    row_reference.kind = Kind::absent;
                  } else if (reference_array) {
                    if (reference_array->array->IsValid(i)) {
                      row_reference.kind = Kind::value;
                      row_reference.value
                        = *view_at<time_type>(*reference_array->array, i);
                    } else {
                      row_reference.kind = Kind::null;
                    }
                  } else if (is<null_type>(reference_series.type)) {
                    row_reference.kind = Kind::null;
                  } else {
                    row_reference.kind = Kind::mistyped;
                  }
                  auto row_zone = Option<TimeZone const*>{
                    static_cast<TimeZone const*>(nullptr)};
                  if (timezone_array) {
                    auto name = Option<std::string_view>{};
                    if (timezone_array->array->IsValid(i)) {
                      name = timezone_array->array->GetView(i);
                    }
                    row_zone = zones.resolve(name, parsed_date_fields.absolute,
                                             issues);
                  } else if (not is<null_type>(timezone_series.type)) {
                    if (parsed_date_fields.absolute) {
                      issues.timezone_ignored = true;
                    } else {
                      if (not issues.timezone_type_error) {
                        issues.timezone_type_error
                          = fmt::to_string(timezone_series.type.kind());
                      }
                      row_zone = None{};
                    }
                  }
                  auto parsed
                    = row_zone
                        ? parse_time_row(array.GetView(i), format.inner,
                                         parsed_date_fields, row_reference,
                                         *row_zone, buffer, issues)
                        : Option<time>{};
                  if (parsed) {
                    b->UnsafeAppend(parsed->time_since_epoch().count());
                  } else {
                    b->UnsafeAppendNull();
                  }
                }
                emit_parse_time_issues(
                  issues, subject_expr.get_location(), format.source,
                  reference ? reference->get_location() : location::unknown,
                  fmt::to_string(reference_series.type.kind()),
                  timezone_expr ? timezone_expr->get_location()
                                : location::unknown,
                  ctx);
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
