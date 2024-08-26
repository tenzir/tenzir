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

auto make_round_temporal_options(const duration time_resolution) noexcept
  -> arrow::compute::RoundTemporalOptions {
  using namespace std::chrono;
  using year_period = std::ratio_multiply<days::period, std::ratio<365>>;
  using month_period = std::ratio_multiply<year_period, std::ratio<1, 12>>;
#define TRY_CAST_EXACTLY(ratio, arrow_unit)                                    \
  do {                                                                         \
    if (auto cast_resolution                                                   \
        = std::chrono::duration_cast<std::chrono::duration<int, ratio>>(       \
          time_resolution);                                                    \
        time_resolution                                                        \
        == std::chrono::duration_cast<duration>(cast_resolution)) {            \
      return arrow::compute::RoundTemporalOptions{                             \
        cast_resolution.count(),                                               \
        arrow::compute::CalendarUnit::arrow_unit,                              \
      };                                                                       \
    }                                                                          \
  } while (false)
  TRY_CAST_EXACTLY(year_period, YEAR);
  TRY_CAST_EXACTLY(month_period, MONTH);
  TRY_CAST_EXACTLY(weeks::period, WEEK);
  TRY_CAST_EXACTLY(days::period, DAY);
  TRY_CAST_EXACTLY(hours::period, HOUR);
  TRY_CAST_EXACTLY(minutes::period, MINUTE);
  TRY_CAST_EXACTLY(seconds::period, SECOND);
  TRY_CAST_EXACTLY(milliseconds::period, MILLISECOND);
  TRY_CAST_EXACTLY(microseconds::period, MICROSECOND);
  TRY_CAST_EXACTLY(nanoseconds::period, NANOSECOND);
#undef TRY_CAST_EXACTLY
  // If neither of these casts are working, then we need nanosecond resolution
  // but have a value so large that it cannot be represented by a signed 32-bit
  // integer. In this case we accept the rounding error and take the closest
  // unit we can without overflow.
#define TRY_CAST_APPROXIMATELY(ratio, arrow_unit)                              \
  do {                                                                         \
    if (auto cast_resolution                                                   \
        = std::chrono::duration_cast<std::chrono::duration<uint64_t, ratio>>(  \
          time_resolution);                                                    \
        cast_resolution.count() <= std::numeric_limits<int>::max()) {          \
      return arrow::compute::RoundTemporalOptions{                             \
        static_cast<int>(cast_resolution.count()),                             \
        arrow::compute::CalendarUnit::arrow_unit,                              \
      };                                                                       \
    }                                                                          \
  } while (false)
  TRY_CAST_APPROXIMATELY(nanoseconds::period, NANOSECOND);
  TRY_CAST_APPROXIMATELY(microseconds::period, MICROSECOND);
  TRY_CAST_APPROXIMATELY(milliseconds::period, MILLISECOND);
  TRY_CAST_APPROXIMATELY(seconds::period, SECOND);
  TRY_CAST_APPROXIMATELY(minutes::period, MINUTE);
  TRY_CAST_APPROXIMATELY(hours::period, HOUR);
  TRY_CAST_APPROXIMATELY(days::period, DAY);
  TRY_CAST_APPROXIMATELY(weeks::period, WEEK);
  TRY_CAST_APPROXIMATELY(month_period, MONTH);
  TRY_CAST_APPROXIMATELY(year_period, YEAR);
#undef TRY_CAST_APPROXIMATELY
  TENZIR_UNREACHABLE();
}

} // namespace tenzir
