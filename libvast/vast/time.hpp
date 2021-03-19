//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include <caf/timespan.hpp>
#include <caf/timestamp.hpp>

#include <chrono>
#include <cstdint>

namespace vast {

using days = std::chrono::duration<
  int, std::ratio_multiply<std::ratio<24>, std::chrono::hours::period>>;

using weeks
  = std::chrono::duration<int, std::ratio_multiply<std::ratio<7>, days::period>>;

using years = std::chrono::duration<
  int, std::ratio_multiply<std::ratio<146097, 400>, days::period>>;

using months
  = std::chrono::duration<int, std::ratio_divide<years::period, std::ratio<12>>>;

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

} // namespace vast
