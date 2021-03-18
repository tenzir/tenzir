// SPDX-FileCopyrightText: (c) 2016 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/time.hpp"

#include "vast/data.hpp"

namespace vast {

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

} // namespace vast
