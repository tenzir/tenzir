/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include <chrono>
#include <ctime>
#include <cstring>

#include <date/date.h>

#include "vast/access.hpp"
#include "vast/time.hpp"
#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/numeric/real.hpp"
#include "vast/concept/parseable/string/char_class.hpp"

namespace vast {

// TODO: replace with Howard Hinnant's TZ stuff and then move into
// vast/concept/parseable/std/chrono.

template <class Rep, class Period>
struct duration_parser : parser<duration_parser<Rep, Period>> {
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
    auto unit
      = "nsecs"_p ->* [] { return cast(nanoseconds(1)); }
      | "nsec"_p  ->* [] { return cast(nanoseconds(1)); }
      | "ns"_p    ->* [] { return cast(nanoseconds(1)); }
      | "usecs"_p ->* [] { return cast(microseconds(1)); }
      | "usec"_p  ->* [] { return cast(microseconds(1)); }
      | "us"_p    ->* [] { return cast(microseconds(1)); }
      | "msecs"_p ->* [] { return cast(milliseconds(1)); }
      | "msec"_p  ->* [] { return cast(milliseconds(1)); }
      | "ms"_p    ->* [] { return cast(milliseconds(1)); }
      | "secs"_p  ->* [] { return cast(seconds(1)); }
      | "sec"_p   ->* [] { return cast(seconds(1)); }
      | "s"_p     ->* [] { return cast(seconds(1)); }
      | "mins"_p  ->* [] { return cast(minutes(1)); }
      | "min"_p   ->* [] { return cast(minutes(1)); }
      | "m"_p     ->* [] { return cast(minutes(1)); }
      | "hrs"_p   ->* [] { return cast(hours(1)); }
      | "hours"_p ->* [] { return cast(hours(1)); }
      | "hour"_p  ->* [] { return cast(hours(1)); }
      | "h"_p     ->* [] { return cast(hours(1)); }
      | "days"_p  ->* [] { return cast(hours(24)); }
      | "day"_p   ->* [] { return cast(hours(24)); }
      | "d"_p     ->* [] { return cast(hours(24)); }
      | "weeks"_p ->* [] { return cast(hours(24 * 7)); }
      | "week"_p  ->* [] { return cast(hours(24 * 7)); }
      | "w"_p     ->* [] { return cast(hours(24 * 7)); }
      | "years"_p ->* [] { return cast(hours(24 * 365)); }
      | "year"_p  ->* [] { return cast(hours(24 * 365)); }
      | "y"_p     ->* [] { return cast(hours(24 * 365)); }
      ;
    if constexpr (std::is_same_v<Attribute, unused_type>) {
      auto p = ignore(real_opt_dot) >> ignore(*space) >> unit;
      return p(f, l, unused);
    } else {
      double scale;
      auto multiply = [&](attribute dur) {
        auto result = duration_cast<duration<double, Period>>(dur) * scale;
        return cast(result);
      };
      auto p = real_opt_dot >> ignore(*space) >> unit ->* multiply;
      return p(f, l, scale, x);
    }
  }
};

template <class Rep, class Period>
struct parser_registry<std::chrono::duration<Rep, Period>> {
  using type = duration_parser<Rep, Period>;
};

namespace parsers {

template <class Rep, class Period>
auto const duration = duration_parser<Rep, Period>{};

auto const timespan = duration<vast::timespan::rep, vast::timespan::period>;

} // namespace parsers

struct ymdhms_parser : vast::parser<ymdhms_parser> {
  using attribute = timestamp;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
    using namespace std::chrono;
    auto year = integral_parser<int, 4, 4>{}
                  .with([](auto x) { return x >= 1900; });
    auto mon = integral_parser<int, 2, 2>{}
                 .with([](auto x) { return x >= 1 && x <= 12; });
    auto day = integral_parser<int, 2, 2>{}
                 .with([](auto x) { return x >= 1 && x <= 31; });
    auto hour = integral_parser<int, 2, 2>{}
                 .with([](auto x) { return x >= 0 && x <= 23; });
    auto min = integral_parser<int, 2, 2>{}
                 .with([](auto x) { return x >= 0 && x <= 59; });
    auto sec = parsers::real_opt_dot
                 .with([](auto x) { return x >= 0.0 && x <= 60.0; });
    auto p = year >> '-' >> mon
        >> ~('-' >> day >> ~('+' >> hour >> ~(':' >> min >> ~(':' >> sec))));
    if constexpr (std::is_same_v<Attribute, unused_type>) {
      return p(f, l, unused);
    } else {
      auto secs = 0.0;
      auto mins = 0;
      auto hrs = 0;
      auto dys = 1;
      auto mons = 1;
      auto yrs = 0;
      // Compose to match parser attribute.
      auto ms = std::tie(mins, secs);
      auto hms = std::tie(hrs, ms);
      auto dhms = std::tie(dys, hms);
      if (!p(f, l, yrs, mons, dhms))
        return false;
      date::sys_days ymd = date::year{yrs} / mons / dys;
      auto delta = hours{hrs} + minutes{mins} + double_seconds{secs};
      x = timestamp{ymd} + duration_cast<timespan>(delta);
      return true;
    }
  }
};

namespace parsers {

auto const ymdhms = ymdhms_parser{};

/// Parses a fractional seconds-timestamp as UNIX epoch.
auto const unix_ts = real_opt_dot
  ->* [](double d) {
    using std::chrono::duration_cast;
    return timestamp{duration_cast<vast::timespan>(double_seconds{d})};
  };

} // namespace parsers

struct timestamp_parser : parser<timestamp_parser> {
  using attribute = timestamp;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    using namespace parser_literals;
    auto plus = [](timespan t) { return timestamp::clock::now() + t; };
    auto minus = [](timespan t) { return timestamp::clock::now() - t; };
    auto ws = ignore(*parsers::space);
    auto p
      = parsers::ymdhms
      | '@' >> parsers::unix_ts
      | "now" >> ws >> ( '+' >> ws >> parsers::timespan ->* plus
                       | '-' >> ws >> parsers::timespan ->* minus )
      | "now"_p ->* []() { return timestamp::clock::now(); }
      | "in" >> ws >> parsers::timespan ->* plus
      | (parsers::timespan ->* minus) >> ws >> "ago"
      ;
    return p(f, l, a);
  }
};

template <>
struct parser_registry<timestamp> {
  using type = timestamp_parser;
};

namespace parsers {

auto const timestamp = timestamp_parser{};

} // namespace parsers
} // namespace vast
