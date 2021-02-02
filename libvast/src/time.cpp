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

#include "vast/data.hpp"
#include "vast/time.hpp"

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
