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

#include <chrono>
#include <cstdint>

#include <caf/timespan.hpp>
#include <caf/timestamp.hpp>

namespace vast {

class json;

/// A duration in time with nanosecond resolution.
using caf::timespan;

/// An absolute point in time with nanosecond resolution. It is capable to
/// represent +/- 292 years around the UNIX epoch.
using caf::timestamp;

/// A helper type to represent fractional time stamps in type `double`.
using double_seconds = std::chrono::duration<double, std::ratio<1>>;

bool convert(timespan dur, double& d);
bool convert(timespan dur, json& j);

bool convert(timestamp tp, double& d);
bool convert(timestamp tp, json& j);

} // namespace vast
