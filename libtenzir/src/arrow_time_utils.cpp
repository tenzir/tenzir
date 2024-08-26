//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_time_utils.hpp>
#include <tenzir/detail/assert.hpp>

namespace tenzir {

//TODO: Decide on additional opts such as changing rounding origin.
auto make_round_temporal_options(const duration time_resolution) noexcept
  -> arrow::compute::RoundTemporalOptions {
#define TRY_CAST_EXACTLY(chrono_unit, arrow_unit)                              \
  do {                                                                         \
    if (auto cast_resolution = std::chrono::duration_cast<                     \
          std::chrono::duration<int, std::chrono::chrono_unit::period>>(       \
          time_resolution);                                                    \
        time_resolution                                                        \
        == std::chrono::duration_cast<duration>(cast_resolution)) {            \
      return arrow::compute::RoundTemporalOptions{                             \
        cast_resolution.count(),                                               \
        arrow::compute::CalendarUnit::arrow_unit,                              \
      };                                                                       \
    }                                                                          \
  } while (false)
  TRY_CAST_EXACTLY(years, YEAR);
  TRY_CAST_EXACTLY(months, MONTH);
  TRY_CAST_EXACTLY(weeks, WEEK);
  TRY_CAST_EXACTLY(days, DAY);
  TRY_CAST_EXACTLY(hours, HOUR);
  TRY_CAST_EXACTLY(minutes, MINUTE);
  TRY_CAST_EXACTLY(seconds, SECOND);
  TRY_CAST_EXACTLY(milliseconds, MILLISECOND);
  TRY_CAST_EXACTLY(microseconds, MICROSECOND);
  TRY_CAST_EXACTLY(nanoseconds, NANOSECOND);
#undef TRY_CAST_EXACTLY
  // If neither of these casts are working, then we need nanosecond resolution
  // but have a value so large that it cannot be represented by a signed 32-bit
  // integer. In this case we accept the rounding error and take the closest
  // unit we can without overflow.
#define TRY_CAST_APPROXIMATELY(chrono_unit, arrow_unit)                        \
  do {                                                                         \
    if (auto cast_resolution = std::chrono::duration_cast<                     \
          std::chrono::duration<uint64_t, std::chrono::chrono_unit::period>>(  \
          time_resolution);                                                    \
        cast_resolution.count() <= std::numeric_limits<int>::max()) {          \
      return arrow::compute::RoundTemporalOptions{                             \
        static_cast<int>(cast_resolution.count()),                             \
        arrow::compute::CalendarUnit::arrow_unit,                              \
      };                                                                       \
    }                                                                          \
  } while (false)
  TRY_CAST_APPROXIMATELY(nanoseconds, NANOSECOND);
  TRY_CAST_APPROXIMATELY(microseconds, MICROSECOND);
  TRY_CAST_APPROXIMATELY(milliseconds, MILLISECOND);
  TRY_CAST_APPROXIMATELY(seconds, SECOND);
  TRY_CAST_APPROXIMATELY(minutes, MINUTE);
  TRY_CAST_APPROXIMATELY(hours, HOUR);
  TRY_CAST_APPROXIMATELY(days, DAY);
  TRY_CAST_APPROXIMATELY(weeks, WEEK);
  TRY_CAST_APPROXIMATELY(months, MONTH);
  TRY_CAST_APPROXIMATELY(years, YEAR);
#undef TRY_CAST_APPROXIMATELY
  TENZIR_UNREACHABLE();
}

} // namespace tenzir
