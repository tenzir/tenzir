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
#include <string>

#include "vast/concept/printable/core.hpp"
#include "vast/concept/printable/numeric/integral.hpp"
#include "vast/concept/printable/numeric/real.hpp"
#include "vast/time.hpp"

namespace vast {
namespace policy {

struct adaptive {};
struct fixed {};

} // namespace policy

template <class Rep, class Period, class Policy = policy::adaptive>
struct duration_printer : printer<duration_printer<Rep, Period, Policy>> {
  using attribute = std::chrono::duration<Rep, Period>;

  template <class To, class R, class P>
  static auto is_at_least(std::chrono::duration<R, P> d) {
    return std::chrono::duration_cast<To>(std::chrono::abs(d)) >= To{1};
  }

  template <class To, class R, class P>
  static auto count(std::chrono::duration<R, P> d) {
    using fractional = std::chrono::duration<double, typename To::period>;
    return std::chrono::duration_cast<fractional>(d).count();
  }

  template <class R, class P>
  static auto units(std::chrono::duration<R, P>) {
    return "not implemented";
  }

#define UNIT_SUFFIX(type, suffix)                                              \
  template <class R>                                                           \
  static auto units(std::chrono::duration<R, type>) {                          \
    return suffix;                                                             \
  }

  UNIT_SUFFIX(std::atto, "as")
  UNIT_SUFFIX(std::femto, "fs")
  UNIT_SUFFIX(std::pico, "ps")
  UNIT_SUFFIX(std::nano, "ns")
  UNIT_SUFFIX(std::micro, "us")
  UNIT_SUFFIX(std::milli, "ms")
  UNIT_SUFFIX(std::centi, "cs")
  UNIT_SUFFIX(std::deci, "ds")
  UNIT_SUFFIX(std::ratio<1>, "s")
  UNIT_SUFFIX(std::deca, "das")
  UNIT_SUFFIX(std::hecto, "hs")
  UNIT_SUFFIX(std::kilo, "ks")
  UNIT_SUFFIX(std::mega, "Ms")
  UNIT_SUFFIX(std::giga, "Gs")
  UNIT_SUFFIX(std::tera, "Ts")
  UNIT_SUFFIX(std::peta, "Ps")
  UNIT_SUFFIX(std::exa, "Es")
  UNIT_SUFFIX(std::ratio<60>, "min")
  UNIT_SUFFIX(std::ratio<3600>, "h")

#undef UNIT_SUFFIX

  template <class Iterator>
  bool print(Iterator& out, std::chrono::duration<Rep, Period> d) const {
    if constexpr (std::is_same_v<Policy, policy::fixed>) {
      auto p = make_printer<Rep>{} << units(d);
      return p(out, d.count());
    } else if constexpr (std::is_same_v<Policy, policy::adaptive>) {
      using namespace std::chrono;
      auto num = printers::real2;
      if (is_at_least<days>(d))
        return (num << 'd')(out, count<days>(d));
      if (is_at_least<hours>(d))
        return (num << 'h')(out, count<hours>(d));
      if (is_at_least<minutes>(d))
        return (num << 'm')(out, count<minutes>(d));
      if (is_at_least<seconds>(d))
        return (num << 's')(out, count<seconds>(d));
      if (is_at_least<milliseconds>(d))
        return (num << "ms")(out, count<milliseconds>(d));
      if (is_at_least<microseconds>(d))
        return (num << "us")(out, count<microseconds>(d));
      return (num << "ns")(out, count<nanoseconds>(d));
    }
  }
};

namespace {

struct year_month_day {
  unsigned short year;
  unsigned char month;
  unsigned char day;
};

// Logic extracted from
// https://github.com/HowardHinnant/date/blob/master/include/date/date.h
// An explanation for this algorithm can be found here:
// http://howardhinnant.github.io/date_algorithms.html#civil_from_days
constexpr year_month_day from_days(days dp) noexcept {
  static_assert(std::numeric_limits<unsigned>::digits >= 18,
                "This algorithm has not been ported to a 16 bit unsigned "
                "integer");
  static_assert(std::numeric_limits<int>::digits >= 20,
                "This algorithm has not been ported to a 16 bit signed "
                "integer");
  auto const z = dp.count() + 719468;
  auto const era = (z >= 0 ? z : z - 146096) / 146097;
  auto const doe = static_cast<unsigned>(z - era * 146097);
  auto const yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
  auto const y = static_cast<days::rep>(yoe) + era * 400;
  auto const doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
  auto const mp = (5 * doy + 2) / 153;
  auto const d = static_cast<unsigned char>(doy - (153 * mp + 2) / 5 + 1);
  auto const m = static_cast<unsigned char>(mp < 10 ? mp + 3 : mp - 9);
  return year_month_day{static_cast<unsigned short>(y + (m <= 2)), m, d};
}

} // namespace

template <class Clock, class Duration>
struct time_point_printer : printer<time_point_printer<Clock, Duration>> {
  using attribute = std::chrono::time_point<Clock, Duration>;

  template <class Iterator>
  bool print(Iterator& out, std::chrono::time_point<Clock, Duration> tp) const {
    using namespace std::chrono;
    auto num = printers::integral<int>;
    auto num2 = printers::integral<int, policy::plain, 2>;
    auto unum2 = printers::integral<unsigned, policy::plain, 2>;
    auto p = num << '-' << unum2 << '-' << unum2
                 << '+' << num2 << ':' << num2 << ':' << num2
                 << '.' << num;
    auto sd = floor<days>(tp);
    auto [Y, M, D] = from_days(duration_cast<days>(sd - timestamp{}));
    auto t = tp - sd;
    auto h = duration_cast<hours>(t);
    auto m = duration_cast<minutes>(t - h);
    auto s = duration_cast<seconds>(t - h - m);
    auto sub_secs = duration_cast<milliseconds>(t - h - m - s);
    return p(out, static_cast<int>(Y), static_cast<unsigned>(M),
             static_cast<unsigned>(D), static_cast<int>(h.count()),
             static_cast<int>(m.count()), static_cast<int>(s.count()),
             static_cast<int>(sub_secs.count()));
  }
};

template <class Rep, class Period>
struct printer_registry<std::chrono::duration<Rep, Period>> {
  using type = duration_printer<Rep, Period>;
};

template <class Clock, class Duration>
struct printer_registry<std::chrono::time_point<Clock, Duration>> {
  using type = time_point_printer<Clock, Duration>;
};

namespace printers {

template <class Duration, class Policy = policy::adaptive>
const auto duration = duration_printer<
  typename Duration::rep,
  typename Duration::period,
  Policy
>{};

template <class Clock, class Duration = class Clock::duration>
const auto time_point = time_point_printer<Clock, Duration>{};

} // namespace printers
} // namespace vast
