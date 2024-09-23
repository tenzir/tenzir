//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define _XOPEN_SOURCE 700 // for strptime

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/detail/posix.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>

#if defined(_LIBCPP_VERSION) && _LIBCPP_VERSION >= 17000
// libc++ v17 or later conflicts with <date.h>
// In that case, we just won't use it, and use an alternative code path,
// which ignores named timezones altogether, which is what we needed <date.h>
// for.
#  include <chrono>
#  define TENZIR_HAS_DATE_H 0
namespace date = std::chrono;
#else
#  include <arrow/vendored/datetime.h>
#  define TENZIR_HAS_DATE_H 1
namespace date = arrow_vendored::date;
#endif

#include <ctime>

namespace tenzir::plugins::time {

namespace {

constexpr auto docs = "https://docs.tenzir.com/formats/time";

#if TENZIR_HAS_DATE_H
auto find_tz_by_name(std::string_view tz_name, diagnostic_handler& diag)
  -> const date::time_zone* {
  const date::tzdb* tzdb{nullptr};
  try {
    tzdb = &date::get_tzdb();
  } catch (const std::runtime_error& ex) {
    diagnostic::error("failed to initialize time zone database")
      .note("message: `{}`", ex.what())
      .emit(diag);
    return nullptr;
  }
  try {
    return tzdb->locate_zone(tz_name);
  } catch (const std::runtime_error& ex) {
    diagnostic::error("failed to initialize time zone")
      .note("time zone: `{}`, message: `{}`", tz_name, ex.what())
      .emit(diag);
    return nullptr;
  }
}
#endif

// Need to give a little nudge using the explicit .operator syntax due to
// incompatibilities with <chrono> and <date.h>
template <typename Clock>
constexpr auto ymd_to_days(date::year_month_day ymd)
  -> std::chrono::time_point<Clock, std::chrono::days> {
  if constexpr (std::is_same_v<Clock, std::chrono::system_clock>) {
    using date::sys_days;
    return ymd.operator sys_days();
  } else {
    using date::local_days;
    using date::sys_days;
    return date::local_days{ymd.operator sys_days().time_since_epoch()};
  }
}

/// Represents a point in time, with possibly some of the information missing.
/// Field names are equivalent to `std::tm`, with the same valid value ranges.
struct partial_timestamp {
  /// Returns a complete `partial_timestamp` representing the first second of
  /// today, i.e. 00:00:00 (UTC)
  static auto today_beginning() -> partial_timestamp {
    auto now = std::chrono::system_clock::now();
    auto ymd
      = std::chrono::year_month_day{std::chrono::floor<std::chrono::days>(now)};
    return partial_timestamp{.tm_sec = 0,
                             .tm_min = 0,
                             .tm_hour = 0,
                             .tm_mday = static_cast<unsigned>(ymd.day()),
                             .tm_mon = static_cast<unsigned>(ymd.month()) - 1,
                             .tm_year = static_cast<int>(ymd.year()) - 1900,
                             .tm_gmtoff = 0,
                             .tm_zone = "UTC"};
  }

  /// Returns a complete `partial_timestamp` with all fields initialized with
  /// fields of `time`
  static auto from_tm(const std::tm& time) -> partial_timestamp {
    return {.tm_sec = time.tm_sec,
            .tm_min = time.tm_min,
            .tm_hour = time.tm_hour,
            .tm_mday = time.tm_mday,
            .tm_mon = time.tm_mon,
            .tm_year = time.tm_year,
            .tm_gmtoff = time.tm_gmtoff,
            .tm_zone = time.tm_zone ? std::optional{std::string{time.tm_zone}}
                                    : std::nullopt};
  }

  /// Returns a possibly-incomplete `partial_timestamp`, with only the fields
  /// initialized for which `is_unset` returned `false`.
  static auto from_tm_with_unset_fields(const std::tm& time, auto is_unset)
    -> partial_timestamp {
    auto do_field = [&]<typename T, typename U>(std::optional<T>& self_field,
                                                const U& other_field) {
      if (not is_unset(other_field))
        self_field = other_field;
    };
    partial_timestamp result{};
    do_field(result.tm_sec, time.tm_sec);
    do_field(result.tm_min, time.tm_min);
    do_field(result.tm_hour, time.tm_hour);
    do_field(result.tm_mday, time.tm_mday);
    do_field(result.tm_mon, time.tm_mon);
    do_field(result.tm_year, time.tm_year);
    do_field(result.tm_gmtoff, time.tm_gmtoff);
    do_field(result.tm_zone, time.tm_zone);
    return result;
  }

  template <typename Clock>
  static auto from_clock_time_point(
    const std::chrono::time_point<Clock, std::chrono::seconds>& tp,
    std::optional<long> gmtoff, std::optional<std::string> tz)
    -> partial_timestamp {
    auto tp_days = std::chrono::floor<std::chrono::days>(tp);
    auto ymd = date::year_month_day{tp_days};
    TENZIR_ASSERT(ymd.ok());
    auto hms = date::hh_mm_ss(
      tp - std::chrono::time_point_cast<std::chrono::seconds>(tp_days));
    return {.tm_sec = hms.seconds().count(),
            .tm_min = hms.minutes().count(),
            .tm_hour = hms.hours().count(),
            .tm_mday = static_cast<unsigned>(ymd.day()),
            .tm_mon = static_cast<unsigned>(ymd.month()) - 1,
            .tm_year = static_cast<int>(ymd.year()) - 1900,
            .tm_gmtoff = gmtoff,
            .tm_zone = std::move(tz)};
  }

  /// Returns a complete `partial_timestamp` corresponding to `tp`, in a timezone.
  static auto from_local_time_point(const date::local_seconds& tp,
                                    std::optional<long> gmtoff,
                                    std::optional<std::string> tz)
    -> partial_timestamp {
    return from_clock_time_point(tp, gmtoff, std::move(tz));
  }

  /// Returns a complete `partial_timestamp` corresponding to `tp`, in UTC.
  static auto from_system_time_point(const std::chrono::sys_seconds& tp)
    -> partial_timestamp {
    return from_clock_time_point(tp, 0, "UTC");
  }

  /// Initializes unset fields in `*this` with the values from `other`.
  /// If `*this` is not UTC, `other` must be complete.
  /// In that case, returns `false` if timezone conversion fails.
  [[nodiscard]] bool enrich(partial_timestamp other, diagnostic_handler& diag) {
    // We need to do some heavy lifting because of timezones.
    // Because `*this` and `other` may be on different timezones, we can't
    // blindly assign from one to another.
    //
    // In general, it's not possible to translate an incomplete timestamp from
    // timezone to another: the most common case is DST, which causes the
    // offsets between two timezones to be dependent on the date.
    //
    // Thus, we'll require `other` to be complete, and translate it to the same
    // timezone as `*this`.
    if (tm_zone && TENZIR_HAS_DATE_H) {
      if (not other.transform_to_tz(*tm_zone, diag))
        return false;
    } else if (tm_gmtoff) {
      if (not other.transform_to_utc_offset(*tm_gmtoff, diag))
        return false;
    }
    // Now, the timezones match, and we can just blindly assign to the members
    auto do_field = []<typename T>(T& self, const T& other_field) {
      if (!self)
        self = other_field;
    };
    do_field(tm_sec, other.tm_sec);
    do_field(tm_min, other.tm_min);
    do_field(tm_hour, other.tm_hour);
    do_field(tm_mday, other.tm_mday);
    do_field(tm_mon, other.tm_mon);
    do_field(tm_year, other.tm_year);
    return true;
  }

  /// Returns `true` if `*this` contains the minimum information needed for a
  /// timestamp, i.e. year, month, day, hour, and minute.
  [[nodiscard]] auto is_complete() const -> bool {
    return tm_min && tm_hour && tm_mday && tm_mon && tm_year;
  }

  /// Returns `true` if `*this` is doesn't have a set timezone, or it's
  /// explicitly UTC/GMT.
  [[nodiscard]] auto is_utc() const -> bool {
    return (not tm_gmtoff || *tm_gmtoff == 0)
           && (not tm_zone || *tm_zone == "UTC" || *tm_zone == "GMT");
  }

  /// Adjusts the members in `*this`, so that `is_utc()` returns `true`.
  /// After calling `transform_to_utc`, both `tm_gmtoff` and `tm_zone` will be
  /// unset, and the time contained is adjusted to be UTC.
  ///
  /// If `tm_zone` is set, it is used to determine the timezone of `*this`.
  /// Otherwise, `tm_gmtoff` is used.
  /// If both are set, they must point to the same timezone.
  ///
  /// Requires `*this` to be complete.
  [[nodiscard]] bool transform_to_utc(diagnostic_handler& diag) {
    TENZIR_ASSERT(is_complete());
    if (is_utc())
      return true;
    auto [local_datetime, utc_offset, tz_name]
      = to_clock_time_point<std::chrono::system_clock>(diag);
    if (not local_datetime)
      return false;
#if TENZIR_HAS_DATE_H
    if (tz_name) {
      // Named timezone, resolve
      auto tz = find_tz_by_name(*tz_name, diag);
      if (not tz)
        return false;
      date::zoned_time datetime{tz, *local_datetime};
      *this = from_system_time_point(datetime.get_sys_time());
      return true;
    }
#endif
    if (utc_offset) {
      // UTC offset, apply
      *local_datetime -= std::chrono::seconds{*utc_offset};
    }
    *this = from_system_time_point(*local_datetime);
    return true;
  }

  /// Adjusts the members in `*this`, so that its timezone UTC offset is
  /// `new_offset`.
  ///
  /// Requires `*this` to be complete.
  [[nodiscard]] bool
  transform_to_utc_offset(long new_offset, diagnostic_handler& diag) {
    TENZIR_ASSERT(is_complete());
    if (not is_utc()) {
      // Not UTC, ensuring consistency between UTC offset and named timezone
      // by first transforming `*this` to UTC
      if (not transform_to_utc(diag))
        return false;
    }
    if (new_offset == 0)
      // UTC was requested
      return true;
    tm_gmtoff = new_offset;
    tm_zone.reset();
    return true;
  }

  /// Adjusts the members in `*this`, so that its timezone UTC offset is
  /// `new_offset`.
  ///
  /// Requires `*this` to be complete, and `tm_gmtoff` to be unset.
  [[nodiscard]] bool
  transform_to_tz(const std::string& new_tz_name, diagnostic_handler& diag) {
    TENZIR_ASSERT(is_complete());
    if (not is_utc()) {
      // Not UTC, ensuring consistency between UTC offset and named timezone
      // by first transforming `*this` to UTC
      if (not transform_to_utc(diag))
        return false;
    }
    if (new_tz_name == "UTC" || new_tz_name == "GMT")
      // UTC was requested
      return true;
    tm_gmtoff.reset();
    tm_zone = new_tz_name;
    return true;
  }

  /// Returns an object of type `std::tm` corresponding to `*this`.
  /// If `*this` is incomplete, returns `std::nullopt`, and emits an error
  /// to `diag`.
  [[nodiscard]] auto to_tm(diagnostic_handler& diag) -> std::optional<std::tm> {
    if (not is_complete()) {
      diagnostic::error("insufficient information to create a datetime")
        .hint("either provide a year, month, day, hour, and minute, or disable "
              "--strict to use default values")
        .docs(docs)
        .emit(diag);
      return std::nullopt;
    }
    return std::tm{.tm_sec = tm_sec.value_or(0),
                   .tm_min = *tm_min,
                   .tm_hour = *tm_hour,
                   .tm_mday = *tm_mday,
                   .tm_mon = *tm_mon,
                   .tm_year = *tm_year,
                   .tm_wday = 0,
                   .tm_yday = 0,
                   .tm_isdst = -1,
                   .tm_gmtoff = tm_gmtoff.value_or(0),
                   .tm_zone = tm_zone ? tm_zone->data() : nullptr};
  }

  template <typename Clock>
  [[nodiscard]] auto to_clock_time_point(diagnostic_handler& diag)
    -> std::tuple<
      std::optional<std::chrono::time_point<Clock, std::chrono::seconds>>,
      std::optional<long>, std::optional<std::string>> {
    auto tm_value = to_tm(diag);
    if (not tm_value)
      return {std::nullopt, std::nullopt, std::nullopt};
    auto time = std::chrono::seconds{tm_value->tm_sec + 60 * tm_value->tm_min
                                     + 60 * 60 * tm_value->tm_hour};
    if (time >= std::chrono::seconds{86400} || time < std::chrono::seconds{0}) {
      diagnostic::error("invalid time").note("value: {}", time).emit(diag);
      return {std::nullopt, std::nullopt, std::nullopt};
    }
    auto date = date::year{tm_value->tm_year + 1900}
                / date::month{static_cast<unsigned>(tm_value->tm_mon + 1)}
                / date::day{static_cast<unsigned>(tm_value->tm_mday)};
    date += date::months{0};
    if (not date.ok()) {
      diagnostic::error("invalid date")
        .note("value: `{}-{}-{}`", static_cast<int>(date.year()),
              static_cast<unsigned>(date.month()),
              static_cast<unsigned>(date.day()))
        .emit(diag);
      return {std::nullopt, std::nullopt, std::nullopt};
    }
    auto datetime = std::chrono::time_point<Clock, std::chrono::seconds>{
      ymd_to_days<Clock>(date) + time};
    return {datetime, tm_gmtoff, tm_zone};
  }

  /// Returns a tuple of the time contained in `*this` in `date::local_seconds`
  /// (local time_point), UTC offset, and timezone name.
  ///
  /// If `*this` is not complete, the first element of the returned tuple is
  /// `std::nullopt`.
  [[nodiscard]] auto to_local_time_point(diagnostic_handler& diag)
    -> std::tuple<std::optional<date::local_seconds>, std::optional<long>,
                  std::optional<std::string>> {
    return to_clock_time_point<date::local_t>(diag);
  }

  /// Returns an object of type `std::chrono::sys_seconds` corresponding to
  /// `*this`.
  ///
  /// If `is_complete()` is `false`, returns `std::nullopt`, and
  /// emits an error to `diag`.
  /// `*this` must be in UTC.
  auto to_system_time_point(diagnostic_handler& diag)
    -> std::optional<std::chrono::sys_seconds> {
    auto [tp, _1, _2] = to_clock_time_point<std::chrono::system_clock>(diag);
    if (not tp)
      return std::nullopt;
    TENZIR_ASSERT(is_utc());
    return *tp;
  }

  void to_record(record_ref& builder) const {
    auto add_field_if_set
      = [&]<typename T>(std::string_view name, const std::optional<T>& val,
                        std::function<T(T)> mod = std::identity{}) {
          if (val)
            builder.field(name, mod(*val));
          else
            builder.field(name, caf::none);
        };
    add_field_if_set("second", tm_sec);
    add_field_if_set("minute", tm_min);
    add_field_if_set("hour", tm_hour);
    add_field_if_set("day", tm_mday);
    add_field_if_set("month", tm_mon, {[](int mon) {
                       return mon + 1;
                     }});
    add_field_if_set("year", tm_year, {[](int y) {
                       return y + 1900;
                     }});
    // Special logic for timezones, to ensure consistency between
    // utc_offset and tz, and to default to UTC
    if (is_utc()) {
      builder.field("utc_offset", 0);
      builder.field("timezone", "UTC");
    } else if (tm_zone && TENZIR_HAS_DATE_H) {
      add_field_if_set("utc_offset", tm_gmtoff);
      builder.field("timezone", *tm_zone);
    } else if (tm_gmtoff) {
      builder.field("utc_offset", *tm_gmtoff);
#if TENZIR_HAS_DATE_H
      add_field_if_set("timezone", tm_zone);
#else
      builder.field("timezone", caf::none);
#endif
    } else {
      builder.field("utc_offset", 0);
      builder.field("timezone", "UTC");
    }
  }

  std::optional<int> tm_sec;
  std::optional<int> tm_min;
  std::optional<int> tm_hour;
  std::optional<int> tm_mday;
  std::optional<int> tm_mon;
  std::optional<int> tm_year;
  std::optional<long> tm_gmtoff;
  std::optional<std::string> tm_zone;
};

auto strptime_partial(diagnostic_handler& diag, const char* input,
                      const char* format) -> std::optional<partial_timestamp> {
  // Using INT_MAX as the placeholder, as it's out-of-range for all the fields,
  // and 0 has a valid meaning for some of them
  // (e.g. 0 seconds is a valid result).
  std::tm time = {.tm_sec = std::numeric_limits<int>::max(),
                  .tm_min = std::numeric_limits<int>::max(),
                  .tm_hour = std::numeric_limits<int>::max(),
                  .tm_mday = std::numeric_limits<int>::max(),
                  .tm_mon = std::numeric_limits<int>::max(),
                  .tm_year = std::numeric_limits<int>::max(),
                  .tm_wday = std::numeric_limits<int>::max(),
                  .tm_yday = std::numeric_limits<int>::max(),
                  .tm_isdst = std::numeric_limits<int>::max(),
                  .tm_gmtoff = std::numeric_limits<long>::max(),
                  .tm_zone = nullptr};
  auto is_unset = []<typename T>(T val) {
    if constexpr (std::is_pointer_v<T>) {
      return val == nullptr;
    } else {
      return val == std::numeric_limits<T>::max();
    }
  };
  errno = 0;
  // Using strptime, which is a POSIX function, available both on Linux and
  // macOS.
  //
  // Unable to use date::parse, because it can't parse partial timestamps,
  // only complete, valid time_points, or calendar dates, like year_month_day.
  // std::chrono::parse is equivalent to date::parse, but it isn't available in
  // our stdlibs (libstdc++ v14 or newer, not on libc++ at all).
  //
  // This is quite unfortunate, because strptime is a little inconsistent across
  // platforms, and doesn't portably have all the features date::parse has.
  //
  // TODO: Assuming "C" locale, is that true for us?
  const auto* result = ::strptime(input, format, &time);
  if (not result) {
    diagnostic::error("failed to parse time")
      .note("strptime error: `{}`", detail::describe_errno(errno))
      .hint("input: `{}`, format: `{}`", input, format)
      .emit(diag);
    return std::nullopt;
  }
  if (result != input + std::strlen(input)) {
    diagnostic::error("failed to parse time")
      .note("format string not exhaustive (`{}` not parsed)", result)
      .hint("input: `{}`, format: `{}`", input, format)
      .emit(diag);
    return std::nullopt;
  }
  return partial_timestamp::from_tm_with_unset_fields(time, is_unset);
}

class time_parser final : public plugin_parser {
public:
  time_parser() = default;

  explicit time_parser(parser_interface& p) {
    auto parser = argument_parser{"time", docs};
    located<std::string> format{};
    parser.add(format, "<format>");
    parser.add("--components", components_);
    parser.add("--strict", strict_);
    parser.parse(p);
    format_ = std::move(format.inner);
  }

  auto name() const -> std::string override {
    return "time";
  }

  auto instantiate(generator<chunk_ptr> input, exec_ctx ctx) const
    -> std::optional<generator<table_slice>> override {
    (void)input;
    diagnostic::error("`{}` cannot be used here", name())
      .emit(ctrl.diagnostics());
    return {};
  }

  auto parse_strings(std::shared_ptr<arrow::StringArray> input,
                     exec_ctx ctx) const -> std::vector<series> override {
    auto b = series_builder{type{record_type{}}};
    for (auto&& string : values(string_type{}, *input)) {
      if (not string) {
        b.null();
        continue;
      }
      auto& diag = ctrl.diagnostics();
      auto time
        = strptime_partial(diag, std::string{*string}.c_str(), format_.c_str());
      if (not time)
        return {};
      if (not strict_) {
        // If --strict is not set,
        // we "enrich" `time` with the first second of today.
        //
        // In other words, if some of the fields of `time` weren't set by
        // `strptime_partial`, we'll set them to a reasonable-ish default, that
        // being today at 00:00:00 UTC.
        //
        // If --strict is set:
        //  - but --components is: the missing fields will just be `null`
        //  - but --components is NOT: error, as there's not enough information
        //                             to create a timestamp
        const bool year_set = time->tm_year.has_value();
        auto today_beginning = partial_timestamp::today_beginning();
        if (not time->enrich(today_beginning, diag))
          return {};
        TENZIR_ASSERT(time->is_complete());
        // A special case:
        // If `strptime_partial` didn't set the year, but the resulting enriched
        // time would be in the future, subtract 1 from the year.
        //
        // This is done to better support RFC 3424 syslog timestamps, which
        // don't encode the year. In general, it's quite reasonable to assume,
        // that the dates we encounter here are meant to be set in the past.
        //
        // Example:
        // If today is 2023-12-14, but we parse "Dec 24th", we'll assume
        // that to mean 2022-12-24. Instead, if we parse "Nov 24th", that'll
        // become 2023-11-24.
        if (not year_set) {
          if (not time->transform_to_utc(diag))
            return {};
          TENZIR_ASSERT(today_beginning.is_utc());
          auto time_tp = time->to_system_time_point(diag);
          auto today_beg_tp = today_beginning.to_system_time_point(diag);
          if (not time_tp || not today_beg_tp)
            return {};
          if (std::chrono::floor<std::chrono::days>(*time_tp) > *today_beg_tp)
            *time->tm_year -= 1;
        }
      }
      auto builder = b.record();
      // Transform to UTC, if able.
      // `to_time_point` (in the non --components branch below) requires UTC,
      // and `to_record` can yield more useful results, if the client code
      // doesn't have to deal with timezones.
      if (time->is_complete() && not time->transform_to_utc(diag))
        return {};
      if (components_) {
        // --components is ON:
        // yield a record with the parsed components
        time->to_record(builder);
      } else {
        // --components is OFF:
        // create a timestamp (sys_seconds time_point)
        auto tp = time->to_system_time_point(diag);
        if (not tp)
          return {};
        // TODO: Preferably, I'd do b.data(tp) here, but `parse` expects a record
        builder.field("timestamp", *tp);
      }
    }
    return b.finish();
  }

  friend auto inspect(auto& f, time_parser& x) -> bool {
    return f.object(x)
      .pretty_name("time_parser")
      .fields(f.field("format", x.format_),
              f.field("components", x.components_),
              f.field("strict", x.strict_));
  }

private:
  std::string format_;
  bool components_{false};
  bool strict_{false};
};

class plugin final : public virtual parser_plugin<time_parser> {
public:
  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    return std::make_unique<time_parser>(p);
  }
};

} // namespace

} // namespace tenzir::plugins::time

TENZIR_REGISTER_PLUGIN(tenzir::plugins::time::plugin)
