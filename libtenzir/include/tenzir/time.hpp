//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include <caf/timespan.hpp>
#include <caf/timestamp.hpp>

#include <chrono>
#include <cstdint>

namespace tenzir {

using std::chrono::days;

using std::chrono::weeks;

using std::chrono::months;

using std::chrono::years;

// time_point

template <class Duration>
using sys_time = std::chrono::time_point<std::chrono::system_clock, Duration>;

using sys_days = sys_time<days>;
using sys_seconds = sys_time<std::chrono::seconds>;

/// A helper type to represent fractional time stamps in type `double`.
using double_seconds = std::chrono::duration<double, std::ratio<1>>;

bool convert(duration dur, double& d);
bool convert(duration dur, data& d);

bool convert(time tp, double& d);
bool convert(time tp, data& d);

template <class Clock, class Duration>
constexpr auto floor(std::chrono::time_point<Clock, Duration> t, duration d) {
  return t - t.time_since_epoch() % d;
}

template <class Rep, class Period>
constexpr auto floor(std::chrono::duration<Rep, Period> t, duration d) {
  return t - t % d;
}

template <class Clock, class Duration>
constexpr auto ceil(std::chrono::time_point<Clock, Duration> t, duration d) {
  auto result = floor(t, d);
  if (result != t) {
    result += d;
  }
  return result;
}

template <class Rep, class Period>
constexpr auto ceil(std::chrono::duration<Rep, Period> t, duration d) {
  auto result = floor(t, d);
  if (result != t) {
    result += d;
  }
  return result;
}

} // namespace tenzir
