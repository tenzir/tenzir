//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/option.hpp"

#include <cstdint>

namespace tenzir {

/// The direction in which a `TimeRounder` moves a value that is not already
/// aligned to its resolution. `round` breaks ties towards the larger value.
enum class TimeRounding { floor, ceil, round };

/// Rounds times and durations to a resolution.
///
/// Durations round to multiples of the resolution. For times, in UTC, the
/// resolution decides how they align:
/// - A multiple of a year (365.2425 days) aligns to January 1 of years whose
///   number is a multiple of it, so `10y` yields decades such as 2020.
/// - A multiple of a month (a twelfth of a year) aligns to the first of a
///   month, counted in multiples since January 1970.
/// - A multiple of a week aligns to Mondays, counted in multiples since
///   Monday, December 29, 1969.
/// - Any other resolution aligns to its multiples since the Unix epoch.
///
/// This matches the alignment of Arrow's temporal rounding functions with
/// their default options, except that `ceil` leaves a time that already sits
/// on a month or year boundary unchanged.
class TimeRounder {
public:
  /// Prepares rounding to `resolution`, which must not be zero. Its sign is
  /// ignored.
  explicit TimeRounder(duration resolution);

  /// Rounds `value`, or returns `None` if the result is outside the range of
  /// `time`.
  auto operator()(time value, TimeRounding mode) const -> Option<time>;

  /// Rounds `value`, or returns `None` if the result is outside the range of
  /// `duration`.
  auto operator()(duration value, TimeRounding mode) const -> Option<duration>;

private:
  /// The magnitude of the resolution in nanoseconds.
  uint64_t resolution_ = 1;
  /// Whether times align to a grid of months rather than of nanoseconds.
  bool months_ = false;
  /// The spacing of the grid that times align to, in its unit.
  uint64_t multiple_ = 1;
  /// The grid line that times align to, in its unit: months since January of
  /// year zero, or nanoseconds since the epoch.
  int64_t origin_ = 0;
};

} // namespace tenzir
