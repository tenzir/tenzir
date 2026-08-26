//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/time.hpp"

#include "tenzir/checked_math.hpp"
#include "tenzir/data.hpp"

#include <cmath>

namespace tenzir {

using std::chrono::duration_cast;

auto from_unix_timestamp(double seconds) -> Option<time> {
  if (not std::isfinite(seconds)) {
    return None{};
  }
  auto whole_seconds = double{};
  auto const fractional_seconds = std::modf(seconds, &whole_seconds);
  constexpr auto ticks_per_second
    = duration_cast<duration>(std::chrono::seconds{1}).count();
  constexpr auto min_whole_seconds = duration::min().count() / ticks_per_second;
  constexpr auto max_whole_seconds = duration::max().count() / ticks_per_second;
  if (whole_seconds < static_cast<double>(min_whole_seconds)
      or whole_seconds > static_cast<double>(max_whole_seconds)) {
    return None{};
  }
  auto const whole_ticks
    = static_cast<duration::rep>(whole_seconds) * ticks_per_second;
  auto const fractional_ticks
    = static_cast<duration::rep>(fractional_seconds * ticks_per_second);
  auto ticks = checked_add(whole_ticks, fractional_ticks);
  if (not ticks) {
    return None{};
  }
  return time{duration{*ticks}};
}

bool convert(duration dur, double& d) {
  d = duration_cast<double_seconds>(dur).count();
  return true;
}

bool convert(duration dur, data& d) {
  double time_since_epoch;
  if (not convert(dur, time_since_epoch)) {
    return false;
  }
  d = time_since_epoch;
  return true;
}

bool convert(time ts, double& d) {
  return convert(ts.time_since_epoch(), d);
}

bool convert(time ts, data& d) {
  return convert(ts.time_since_epoch(), d);
}

} // namespace tenzir
