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

#include <date/date.h>

#include "vast/concept/printable/core.hpp"
#include "vast/concept/printable/string/any.hpp"
#include "vast/concept/printable/string/string.hpp"
#include "vast/concept/printable/numeric/integral.hpp"
#include "vast/concept/printable/numeric/real.hpp"
#include "vast/detail/chrono_io.hpp"
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
    return std::chrono::duration_cast<To>(date::abs(d)) >= To{1};
  }

  template <class To, class R, class P>
  static auto count(std::chrono::duration<R, P> d) {
    using fractional = std::chrono::duration<double, typename To::period>;
    return std::chrono::duration_cast<fractional>(d).count();
  }

  template <class R, class P>
  static std::string units(std::chrono::duration<R, P>) {
    return date::detail::get_units<char>(Period{}).data();
  }

  template <class R>
  static std::string units(std::chrono::duration<R, std::micro>) {
    return "us";
  }

  template <class Iterator>
  bool print(Iterator& out, std::chrono::duration<Rep, Period> d) const {
    if constexpr (std::is_same_v<Policy, policy::fixed>) {
      auto p = make_printer<Rep>{} << units(d);
      return p(out, d.count());
    } else if constexpr (std::is_same_v<Policy, policy::adaptive>) {
      using namespace std::chrono;
      using namespace date;
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

template <class Clock, class Duration>
struct time_point_printer : printer<time_point_printer<Clock, Duration>> {
  using attribute = std::chrono::time_point<Clock, Duration>;

  template <class Iterator>
  bool print(Iterator& out, std::chrono::time_point<Clock, Duration> tp) const {
    using namespace std::chrono;
    using namespace date;
    auto num = printers::integral<int>;
    auto num2 = printers::integral<int, policy::plain, 2>;
    auto unum2 = printers::integral<unsigned, policy::plain, 2>;
    auto p = num << '-' << unum2 << '-' << unum2
                 << '+' << num2 << ':' << num2 << ':' << num2
                 << '.' << num;
    auto sd = floor<days>(tp);
    auto ymd = year_month_day{sd};
    auto t = make_time(tp - sd);
    auto sub_secs = duration_cast<milliseconds>(t.subseconds());
    return p(out,
             static_cast<int>(ymd.year()),
             static_cast<unsigned>(ymd.month()),
             static_cast<unsigned>(ymd.day()),
             static_cast<int>(t.hours().count()),
             static_cast<int>(t.minutes().count()),
             static_cast<int>(t.seconds().count()),
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


