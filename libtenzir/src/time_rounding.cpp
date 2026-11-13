//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/time_rounding.hpp"

#include "tenzir/detail/assert.hpp"

#include <chrono>
#include <limits>

namespace tenzir {

namespace {

using Wide = __int128_t;

constexpr auto nanos_per_day = int64_t{86'400'000'000'000};
constexpr auto nanos_per_week = uint64_t{7 * nanos_per_day};
constexpr auto nanos_per_year = static_cast<uint64_t>(
  std::chrono::nanoseconds{std::chrono::years{1}}.count());
constexpr auto nanos_per_month = static_cast<uint64_t>(
  std::chrono::nanoseconds{std::chrono::months{1}}.count());

/// Monday, December 29, 1969, the last Monday before the Unix epoch.
constexpr auto week_origin = -3 * nanos_per_day;

/// January 1970, in months since January of year zero.
constexpr auto month_origin = int64_t{1970 * 12};

auto floor_div(Wide x, Wide y) -> Wide {
  auto const q = x / y;
  return (x % y != 0 and (x < 0) != (y < 0)) ? q - 1 : q;
}

/// The start of month `index`, counted since January of year zero, in
/// nanoseconds since the epoch.
auto month_start(Wide index) -> Wide {
  auto const y = floor_div(index, 12);
  auto const m = static_cast<unsigned>(index - y * 12 + 1);
  auto const days = std::chrono::sys_days{std::chrono::year{static_cast<int>(y)}
                                          / std::chrono::month{m} / 1};
  return Wide{days.time_since_epoch().count()} * nanos_per_day;
}

/// Picks the bound that `mode` rounds `value` to, or returns `None` if it does
/// not fit into 64 bits.
auto pick(Wide value, Wide lower, Wide upper, TimeRounding mode)
  -> Option<int64_t> {
  auto const result = [&] {
    if (lower == value) {
      return value;
    }
    switch (mode) {
      case TimeRounding::floor:
        return lower;
      case TimeRounding::ceil:
        return upper;
      case TimeRounding::round:
        return value - lower >= upper - value ? upper : lower;
    }
    TENZIR_UNREACHABLE();
  }();
  if (result < std::numeric_limits<int64_t>::min()
      or result > std::numeric_limits<int64_t>::max()) {
    return None{};
  }
  return static_cast<int64_t>(result);
}

} // namespace

TimeRounder::TimeRounder(duration resolution) {
  TENZIR_ASSERT(resolution.count() != 0);
  auto const count = resolution.count();
  // Negating in unsigned arithmetic keeps the minimum representable.
  resolution_ = count < 0 ? uint64_t{0} - static_cast<uint64_t>(count)
                          : static_cast<uint64_t>(count);
  // A year grid counts months from year zero, so that a multiple of years
  // aligns to year numbers divisible by it.
  if (resolution_ % nanos_per_year == 0) {
    months_ = true;
    multiple_ = resolution_ / nanos_per_year * 12;
  } else if (resolution_ % nanos_per_month == 0) {
    months_ = true;
    multiple_ = resolution_ / nanos_per_month;
    origin_ = month_origin;
  } else {
    multiple_ = resolution_;
    origin_ = resolution_ % nanos_per_week == 0 ? week_origin : 0;
  }
}

auto TimeRounder::operator()(time value, TimeRounding mode) const
  -> Option<time> {
  auto const nanos = value.time_since_epoch().count();
  auto const multiple = Wide{multiple_};
  auto lower = Wide{};
  auto upper = Wide{};
  if (months_) {
    auto const days = static_cast<int>(floor_div(nanos, nanos_per_day));
    auto const ymd = std::chrono::year_month_day{
      std::chrono::sys_days{std::chrono::days{days}}};
    auto const index = Wide{static_cast<int>(ymd.year())} * 12
                       + static_cast<unsigned>(ymd.month()) - 1;
    auto const floored
      = origin_ + floor_div(index - origin_, multiple) * multiple;
    lower = month_start(floored);
    upper = month_start(floored + multiple);
  } else {
    lower = origin_ + floor_div(nanos - origin_, multiple) * multiple;
    upper = lower + multiple;
  }
  return pick(nanos, lower, upper, mode).transform([](int64_t result) {
    return time{duration{result}};
  });
}

auto TimeRounder::operator()(duration value, TimeRounding mode) const
  -> Option<duration> {
  auto const resolution = Wide{resolution_};
  auto const lower = floor_div(value.count(), resolution) * resolution;
  return pick(value.count(), lower, lower + resolution, mode)
    .transform([](int64_t result) {
      return duration{result};
    });
}

} // namespace tenzir
