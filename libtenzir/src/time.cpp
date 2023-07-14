//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/time.hpp"

#include "tenzir/data.hpp"

namespace tenzir {

using std::chrono::duration_cast;

bool convert(duration dur, double& d) {
  d = duration_cast<double_seconds>(dur).count();
  return true;
}

bool convert(duration dur, data& d) {
  double time_since_epoch;
  if (!convert(dur, time_since_epoch))
    return false;
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
