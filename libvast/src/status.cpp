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

#include "vast/status.hpp"

namespace vast {

caf::dictionary<caf::config_value> join(const status& s) {
  caf::dictionary<caf::config_value> result;
  result["info"] = s.info;
  result["verbose"] = s.verbose;
  result["debug"] = s.debug;
  return result;
}

} // namespace vast
