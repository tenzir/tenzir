//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/time_rounding.hpp"

#include "tenzir/arrow_memory_pool.hpp"
#include "tenzir/arrow_time_utils.hpp"
#include "tenzir/arrow_utils.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/test/test.hpp"

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/compute/initialize.h>
#include <fmt/format.h>

#include <chrono>
#include <cstdint>
#include <limits>
#include <random>
#include <string>
#include <vector>

namespace tenzir {

using namespace std::chrono_literals;

namespace {

auto at(std::chrono::year_month_day ymd, duration time_of_day = {}) -> time {
  return time{std::chrono::sys_days{ymd}.time_since_epoch() + time_of_day};
}

/// Formats a rounding of `value` to `resolution` and its outcome, so that a
/// mismatch names its inputs.
auto describe(time value, duration resolution, TimeRounding mode,
              Option<time> result) -> std::string {
  auto const* name = mode == TimeRounding::floor  ? "floor"
                     : mode == TimeRounding::ceil ? "ceil"
                                                  : "round";
  auto const outcome
    = result ? fmt::to_string(result->time_since_epoch().count()) : "overflow";
  return fmt::format("{}({}ns, {}ns) = {}", name,
                     value.time_since_epoch().count(), resolution.count(),
                     outcome);
}

auto round_to(time value, duration resolution, TimeRounding mode)
  -> Option<time> {
  return TimeRounder{resolution}(value, mode);
}

/// Rounds `values` through Arrow's temporal rounding functions.
auto arrow_round(std::vector<time> const& values, duration resolution,
                 TimeRounding mode) -> std::vector<time> {
  auto builder = arrow::TimestampBuilder{
    arrow::timestamp(arrow::TimeUnit::NANO), arrow_memory_pool()};
  for (auto value : values) {
    check(builder.Append(value.time_since_epoch().count()));
  }
  auto input = finish(builder);
  auto options = make_round_temporal_options(resolution);
  auto output = [&] {
    switch (mode) {
      case TimeRounding::floor:
        return arrow::compute::FloorTemporal(input, options);
      case TimeRounding::ceil:
        return arrow::compute::CeilTemporal(input, options);
      case TimeRounding::round:
        return arrow::compute::RoundTemporal(input, options);
    }
    TENZIR_UNREACHABLE();
  }();
  auto array = check(std::move(output)).make_array();
  auto const& timestamps = static_cast<arrow::TimestampArray const&>(*array);
  auto result = std::vector<time>{};
  for (auto i = int64_t{0}; i < timestamps.length(); ++i) {
    result.emplace_back(duration{timestamps.Value(i)});
  }
  return result;
}

} // namespace

TEST("time rounding aligns to calendar units") {
  using std::chrono::August;
  using std::chrono::December;
  using std::chrono::February;
  using std::chrono::January;
  using std::chrono::July;
  using std::chrono::September;
  using std::chrono::year;
  auto const t = at(year{2024} / August / 27, 16h + 42min + 43s);
  auto const year_ = duration{std::chrono::years{1}};
  auto const month = duration{std::chrono::months{1}};
  CHECK_EQUAL(round_to(t, year_, TimeRounding::round),
              at(year{2025} / January / 1));
  CHECK_EQUAL(round_to(t, year_ / 2, TimeRounding::round),
              at(year{2024} / July / 1));
  CHECK_EQUAL(round_to(t, 10 * year_, TimeRounding::floor),
              at(year{2020} / January / 1));
  // Like Arrow, multi-year grids count from year zero, not from 1970.
  CHECK_EQUAL(round_to(t, 3 * year_, TimeRounding::floor),
              at(year{2022} / January / 1));
  CHECK_EQUAL(round_to(t, month, TimeRounding::round),
              at(year{2024} / September / 1));
  CHECK_EQUAL(round_to(t, std::chrono::weeks{1}, TimeRounding::floor),
              at(year{2024} / August / 26));
  CHECK_EQUAL(round_to(t, 30s, TimeRounding::round),
              at(year{2024} / August / 27, 16h + 42min + 30s));
  // Times before the epoch align to the same grid.
  CHECK_EQUAL(round_to(at(year{1969} / December / 31), std::chrono::weeks{1},
                       TimeRounding::floor),
              at(year{1969} / December / 29));
  CHECK_EQUAL(round_to(at(year{1969} / December / 31, 1h), month,
                       TimeRounding::floor),
              at(year{1969} / December / 1));
  // The sign of the resolution does not matter.
  CHECK_EQUAL(round_to(t, -30s, TimeRounding::round),
              round_to(t, 30s, TimeRounding::round));
  CHECK_EQUAL(round_to(t, -month, TimeRounding::ceil),
              at(year{2024} / September / 1));
  // `ceil` keeps a time that is already on a boundary.
  auto const boundary = at(year{2024} / February / 1);
  CHECK_EQUAL(round_to(boundary, month, TimeRounding::ceil), boundary);
  CHECK_EQUAL(round_to(at(year{2024} / January / 1), year_, TimeRounding::ceil),
              at(year{2024} / January / 1));
  CHECK_EQUAL(round_to(boundary, 1h, TimeRounding::ceil), boundary);
  // Ties round up.
  CHECK_EQUAL(round_to(at(year{2024} / February / 1, 30s), 1min,
                       TimeRounding::round),
              at(year{2024} / February / 1, 1min));
}

TEST("time rounding reports results outside the range of time") {
  auto const max = time{duration{std::numeric_limits<int64_t>::max()}};
  auto const min = time{duration{std::numeric_limits<int64_t>::min()}};
  auto const year_ = duration{std::chrono::years{1}};
  CHECK_EQUAL(round_to(max, std::chrono::days{1}, TimeRounding::ceil), None{});
  CHECK_EQUAL(round_to(max, year_, TimeRounding::ceil), None{});
  CHECK_EQUAL(round_to(min, std::chrono::days{1}, TimeRounding::floor), None{});
  CHECK_EQUAL(round_to(min, duration{std::chrono::months{1}},
                       TimeRounding::floor),
              None{});
  CHECK_EQUAL(round_to(max, duration{std::numeric_limits<int64_t>::min()},
                       TimeRounding::floor),
              time{});
  CHECK(round_to(max, std::chrono::days{1}, TimeRounding::floor).has_value());
  CHECK(round_to(min, std::chrono::days{1}, TimeRounding::ceil).has_value());
}

TEST("time rounding matches Arrow's temporal rounding") {
  REQUIRE(arrow::compute::Initialize().ok());
  using std::chrono::year;
  auto values = std::vector<time>{
    time{},
    time{-1ns},
    time{1ns},
    at(year{1969} / std::chrono::December / 29),
    at(year{1970} / std::chrono::January / 5),
    at(year{2000} / std::chrono::February / 29, 12h),
    at(year{2024} / std::chrono::March / 1),
    at(year{2024} / std::chrono::August / 27, 16h + 42min + 43s),
  };
  // Stay clear of the limits of `time`, where Arrow overflows silently.
  auto const lowest = at(year{1700} / std::chrono::January / 1);
  auto const highest = at(year{2240} / std::chrono::January / 1);
  auto engine = std::mt19937_64{42};
  auto distribution = std::uniform_int_distribution<int64_t>{
    lowest.time_since_epoch().count(), highest.time_since_epoch().count()};
  for (auto i = 0; i < 2000; ++i) {
    values.emplace_back(duration{distribution(engine)});
  }
  auto const resolutions = std::vector<duration>{
    1ns,
    7ns,
    1us,
    250ms,
    1s,
    7s,
    30s,
    1min,
    15min,
    1h,
    5h,
    36h,
    std::chrono::days{1},
    std::chrono::days{3},
    std::chrono::days{30},
    std::chrono::weeks{1},
    std::chrono::weeks{2},
    std::chrono::weeks{5},
    std::chrono::months{1},
    std::chrono::months{2},
    std::chrono::months{5},
    std::chrono::months{6},
    std::chrono::years{1},
    std::chrono::years{2},
    std::chrono::years{3},
    std::chrono::years{7},
    std::chrono::years{10},
  };
  for (auto resolution : resolutions) {
    for (auto mode :
         {TimeRounding::floor, TimeRounding::ceil, TimeRounding::round}) {
      auto const expected = arrow_round(values, resolution, mode);
      for (auto i = size_t{0}; i < values.size(); ++i) {
        auto const actual = round_to(values[i], resolution, mode);
        // Arrow moves a time on a month or year boundary to the next one when
        // taking the ceiling; we keep it.
        if (mode == TimeRounding::ceil
            and round_to(values[i], resolution, TimeRounding::floor)
                  == values[i]) {
          CHECK_EQUAL(describe(values[i], resolution, mode, actual),
                      describe(values[i], resolution, mode, values[i]));
          continue;
        }
        CHECK_EQUAL(describe(values[i], resolution, mode, actual),
                    describe(values[i], resolution, mode, expected[i]));
      }
    }
  }
}

} // namespace tenzir
