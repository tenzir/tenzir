//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/access.hpp"
#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/numeric/real.hpp"
#include "vast/concept/parseable/string/char_class.hpp"
#include "vast/time.hpp"

#include <chrono>
#include <cstring>
#include <ctime>

namespace vast {

// TODO: replace with Howard Hinnant's TZ stuff and then move into
// vast/concept/parseable/std/chrono.

template <class Rep, class Period>
struct duration_parser : parser_base<duration_parser<Rep, Period>> {
  using attribute = std::chrono::duration<Rep, Period>;

  template <class T>
  static attribute cast(T x) {
    return std::chrono::duration_cast<attribute>(x);
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
    using namespace parsers;
    using namespace parser_literals;
    using namespace std::chrono;
    // clang-format off
    auto unit
      = "nanoseconds"_p  ->* [] { return cast(nanoseconds(1)); }
      | "nanosecond"_p   ->* [] { return cast(nanoseconds(1)); }
      | "nsecs"_p        ->* [] { return cast(nanoseconds(1)); }
      | "nsec"_p         ->* [] { return cast(nanoseconds(1)); }
      | "ns"_p           ->* [] { return cast(nanoseconds(1)); }
      | "microseconds"_p ->* [] { return cast(microseconds(1)); }
      | "microsecond"_p  ->* [] { return cast(microseconds(1)); }
      | "usecs"_p        ->* [] { return cast(microseconds(1)); }
      | "usec"_p         ->* [] { return cast(microseconds(1)); }
      | "us"_p           ->* [] { return cast(microseconds(1)); }
      | "milliseconds"_p ->* [] { return cast(milliseconds(1)); }
      | "millisecond"_p  ->* [] { return cast(milliseconds(1)); }
      | "msecs"_p        ->* [] { return cast(milliseconds(1)); }
      | "msec"_p         ->* [] { return cast(milliseconds(1)); }
      | "ms"_p           ->* [] { return cast(milliseconds(1)); }
      | "seconds"_p      ->* [] { return cast(seconds(1)); }
      | "second"_p       ->* [] { return cast(seconds(1)); }
      | "secs"_p         ->* [] { return cast(seconds(1)); }
      | "sec"_p          ->* [] { return cast(seconds(1)); }
      | "s"_p            ->* [] { return cast(seconds(1)); }
      | "minutes"_p      ->* [] { return cast(minutes(1)); }
      | "minute"_p       ->* [] { return cast(minutes(1)); }
      | "mins"_p         ->* [] { return cast(minutes(1)); }
      | "min"_p          ->* [] { return cast(minutes(1)); }
      | "m"_p            ->* [] { return cast(minutes(1)); }
      | "hours"_p        ->* [] { return cast(hours(1)); }
      | "hour"_p         ->* [] { return cast(hours(1)); }
      | "hrs"_p          ->* [] { return cast(hours(1)); }
      | "h"_p            ->* [] { return cast(hours(1)); }
      | "days"_p         ->* [] { return cast(hours(24)); }
      | "day"_p          ->* [] { return cast(hours(24)); }
      | "d"_p            ->* [] { return cast(hours(24)); }
      | "weeks"_p        ->* [] { return cast(hours(24 * 7)); }
      | "week"_p         ->* [] { return cast(hours(24 * 7)); }
      | "w"_p            ->* [] { return cast(hours(24 * 7)); }
      | "years"_p        ->* [] { return cast(hours(24 * 365)); }
      | "year"_p         ->* [] { return cast(hours(24 * 365)); }
      | "y"_p            ->* [] { return cast(hours(24 * 365)); }
      ;
    // clang-format on
    if constexpr (std::is_same_v<Attribute, unused_type>) {
      auto p = ignore(parsers::real) >> ignore(*space) >> unit;
      return p(f, l, unused);
    } else {
      double scale;
      auto multiply = [&](attribute dur) {
        using double_duration = std::chrono::duration<double, Period>;
        auto result = duration_cast<double_duration>(dur) * scale;
        return cast(result);
      };
      auto p = parsers::real >> ignore(*space) >> unit->*multiply;
      return p(f, l, scale, x);
    }
  }
};

template <class Rep, class Period>
struct compound_duration_parser
  : parser_base<compound_duration_parser<Rep, Period>> {
  using attribute = std::chrono::duration<Rep, Period>;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
    auto duration = duration_parser<Rep, Period>{};
    auto guard = [](attribute dur) { return dur.count() > 0; };
    auto positive_duration = duration.with(guard);
    auto add = [&](attribute component) { x += component; };
    auto p = duration >> ignore(*(positive_duration ->* add));
    return p(f, l, x);
  }
};

template <class Rep, class Period>
struct parser_registry<std::chrono::duration<Rep, Period>> {
  using type = compound_duration_parser<Rep, Period>;
};

namespace parsers {

/// A parser template for any duration type from `std::chrono`.
template <class Rep, class Period>
auto const stl_duration = compound_duration_parser<Rep, Period>{};

/// A parser for VASTs duration type.
auto const duration = stl_duration<vast::duration::rep, vast::duration::period>;

} // namespace parsers

// TODO: Support more of ISO8601.
struct ymdhms_parser : vast::parser_base<ymdhms_parser> {
  using attribute = time;

  // Logic extracted from
  // https://github.com/HowardHinnant/date/blob/master/include/date/date.h
  // An explanation for this algorithm can be found here:
  // http://howardhinnant.github.io/date_algorithms.html#days_from_civil
  [[nodiscard]] constexpr sys_days
  to_days(unsigned short year, unsigned char month, unsigned char day) const {
    static_assert(std::numeric_limits<unsigned>::digits >= 18,
                  "This algorithm has not been ported to a 16 bit unsigned "
                  "integer");
    static_assert(std::numeric_limits<int>::digits >= 20,
                  "This algorithm has not been ported to a 16 bit signed "
                  "integer");
    auto const y = static_cast<int>(year) - (month <= 2);
    auto const m = static_cast<unsigned>(month);
    auto const d = static_cast<unsigned>(day);
    auto const era = (y >= 0 ? y : y - 399) / 400;
    auto const yoe = static_cast<unsigned>(y - era * 400);
    auto const doy = (153 * (m > 2 ? m - 3 : m + 9) + 2) / 5 + d - 1;
    auto const doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return sys_days{} + days{era * 146097 + static_cast<int>(doe) - 719468};
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
    using namespace parser_literals;
    using namespace std::chrono;
    auto year = integral_parser<int, 4, 4>{}.with(
      [](auto x) { return x >= 1900; });
    auto mon = integral_parser<int, 2, 2>{}.with(
      [](auto x) { return x >= 1 && x <= 12; });
    auto day = integral_parser<int, 2, 2>{}.with(
      [](auto x) { return x >= 1 && x <= 31; });
    auto hour = integral_parser<int, 2, 2>{}.with(
      [](auto x) { return x >= 0 && x <= 23; });
    auto min = integral_parser<int, 2, 2>{}.with(
      [](auto x) { return x >= 0 && x <= 59; });
    auto sec = parsers::real.with([](auto x) {
      return x >= 0.0 && x <= 60.0;
    });
    auto time_divider = '+'_p | 'T' | ' ';
    // clang-format off
    auto sign = '+'_p ->* [] { return 1; }
              | '-'_p ->* [] { return -1; };
    auto zone = 'Z'
              | (sign >> hour >> ~(~':'_p >> min));
    auto p = year >> '-' >> mon
              >> ~('-' >> day
                >> ~(time_divider >> hour
                  >> ~(':' >> min
                    >> ~(':' >> sec) >> ~zone)));
    // clang-format on
    if constexpr (std::is_same_v<Attribute, unused_type>) {
      return p(f, l, unused);
    } else {
      auto zsign = 1;
      auto zmins = 0;
      auto zhrs = 0;
      auto secs = 0.0;
      auto mins = 0;
      auto hrs = 0;
      auto dys = 1;
      auto mons = 1;
      auto yrs = 0;
      // Compose to match parser attribute.
      auto zshift = std::tie(zsign, zhrs, zmins);
      auto ms = std::tie(mins, secs, zshift);
      auto hms = std::tie(hrs, ms);
      auto dhms = std::tie(dys, hms);
      if (!p(f, l, yrs, mons, dhms))
        return false;
      sys_days ymd = to_days(yrs, mons, dys);
      auto zone_offset = (hours{zhrs} + minutes{zmins}) * zsign;
      auto delta
        = hours{hrs} + minutes{mins} - zone_offset + double_seconds{secs};
      x = time{ymd} + duration_cast<vast::duration>(delta);
      return true;
    }
  }
};

namespace parsers {

auto const ymdhms = ymdhms_parser{};

/// Parses a fractional seconds-timestamp as UNIX epoch.
auto const unix_ts = parsers::real->*[](double d) {
  using std::chrono::duration_cast;
  return time{duration_cast<vast::duration>(double_seconds{d})};
};

} // namespace parsers

struct time_parser : parser_base<time_parser> {
  using attribute = time;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    using namespace parser_literals;
    auto plus = [](duration t) { return time::clock::now() + t; };
    auto minus = [](duration t) { return time::clock::now() - t; };
    auto ws = ignore(*parsers::space);
    auto p = parsers::ymdhms | '@' >> parsers::unix_ts
             | "now" >> ws >> ('+' >> ws >> parsers::duration->*plus
                               | '-' >> ws >> parsers::duration->*minus)
             | "now"_p->*[]() { return time::clock::now(); }
             | "in" >> ws >> parsers::duration->*plus
             | (parsers::duration->*minus) >> ws >> "ago";
    return p(f, l, a);
  }
};

template <>
struct parser_registry<time> {
  using type = time_parser;
};

namespace parsers {

auto const time = time_parser{};

} // namespace parsers
} // namespace vast
