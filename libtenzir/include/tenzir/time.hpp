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

using days = std::chrono::days;

using weeks = std::chrono::weeks;

using months = std::chrono::months;

using years = std::chrono::years;

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

} // namespace tenzir
