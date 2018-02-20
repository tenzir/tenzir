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

#include "vast/json.hpp"
#include "vast/time.hpp"

namespace vast {

using std::chrono::duration_cast;

bool convert(timespan dur, double& d) {
  d = duration_cast<double_seconds>(dur).count();
  return true;
}

bool convert(timespan dur, json& j) {
  j = dur.count();
  return true;
}

bool convert(timestamp ts, double& d) {
  return convert(ts.time_since_epoch(), d);
}

bool convert(timestamp ts, json& j) {
  return convert(ts.time_since_epoch(), j);
}

} // namespace vast
