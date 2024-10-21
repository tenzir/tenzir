//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/access.hpp"
#include "tenzir/concept/parseable/core.hpp"
#include "tenzir/concept/parseable/numeric/real.hpp"
#include "tenzir/concept/parseable/string/char_class.hpp"
#include "tenzir/time.hpp"

#include <chrono>
#include <cstring>
#include <ctime>

namespace tenzir {

// TODO: replace with Howard Hinnant's TZ stuff and then move into
// tenzir/concept/parseable/std/chrono.

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
    using unit_map_type = std::unordered_map<std::string_view, attribute>;
    static const auto unit_map = unit_map_type{
      {"nanoseconds", cast(nanoseconds(1))},
      {"nanosecond", cast(nanoseconds(1))},
      {"nsecs", cast(nanoseconds(1))},
      {"nsec", cast(nanoseconds(1))},
      {"ns", cast(nanoseconds(1))},
      {"microseconds", cast(microseconds(1))},
      {"microsecond", cast(microseconds(1))},
      {"usecs", cast(microseconds(1))},
      {"usec", cast(microseconds(1))},
      {"us", cast(microseconds(1))},
      {"milliseconds", cast(milliseconds(1))},
      {"millisecond", cast(milliseconds(1))},
      {"msecs", cast(milliseconds(1))},
      {"msec", cast(milliseconds(1))},
      {"ms", cast(milliseconds(1))},
      {"seconds", cast(seconds(1))},
      {"second", cast(seconds(1))},
      {"secs", cast(seconds(1))},
      {"sec", cast(seconds(1))},
      {"s", cast(seconds(1))},
      {"minutes", cast(minutes(1))},
      {"minute", cast(minutes(1))},
      {"mins", cast(minutes(1))},
      {"min", cast(minutes(1))},
      {"m", cast(minutes(1))},
      {"hours", cast(hours(1))},
      {"hour", cast(hours(1))},
      {"hrs", cast(hours(1))},
      {"h", cast(hours(1))},
      {"days", cast(hours(24))},
      {"day", cast(hours(24))},
      {"d", cast(hours(24))},
      {"weeks", cast(hours(24 * 7))},
      {"week", cast(hours(24 * 7))},
      {"w", cast(hours(24 * 7))},
      {"years", cast(hours(24 * 365))},
      {"year", cast(hours(24 * 365))},
      {"y", cast(hours(24 * 365))},
    };
    static const auto unit
      = (+parsers::alpha)
          .then([&](std::string str) -> unit_map_type::const_iterator {
            return unit_map.find(str);
          })
          .with([&](unit_map_type::const_iterator it) -> bool {
            return it != unit_map.end();
          })
          .then([](unit_map_type::const_iterator it) -> attribute {
            return it->second;
          });
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
    auto negation = (-parsers::ch<'-'>).then([](std::optional<char> x) -> bool {
      return x.has_value();
    });
    auto positive_duration
      = ignore(&!parsers::ch<'-'>) >> duration_parser<Rep, Period>{};
    auto compound_duration = negation >> (positive_duration % *parsers::space);
    if constexpr (std::is_same_v<Attribute, unused_type>) {
      return compound_duration(f, l, x);
    } else {
      auto result = std::tuple<bool, std::vector<attribute>>{};
      if (not compound_duration(f, l, result)) {
        return false;
      }
      const auto& [negated, parts] = result;
      x = attribute::zero();
      if (negated) {
        for (auto part : parts) {
          x -= part;
        }
      } else {
        for (auto part : parts) {
          x += part;
        }
      }
      return true;
    }
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

/// A parser for Tenzirs duration type.
auto const duration = stl_duration<duration::rep, duration::period>;

template <class Rep, class Period>
auto const simple_stl_duration = duration_parser<Rep, Period>{};

auto const simple_duration = duration_parser<duration::rep, duration::period>{};

} // namespace parsers

// TODO: Support more of ISO8601.
struct ymdhms_parser : tenzir::parser_base<ymdhms_parser> {
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
    auto sec = parsers::real_detect_sep.with([](auto x) {
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
      x = time{ymd} + duration_cast<tenzir::duration>(delta);
      return true;
    }
  }
};

namespace parsers {

auto const ymdhms = ymdhms_parser{};

/// Parses a fractional seconds-timestamp as UNIX epoch.
auto const unix_ts = parsers::real->*[](double d) {
  using std::chrono::duration_cast;
  return time{duration_cast<tenzir::duration>(double_seconds{d})};
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
} // namespace tenzir
