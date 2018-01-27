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

#ifndef VAST_SYSTEM_PROFILER_HPP
#define VAST_SYSTEM_PROFILER_HPP

#include <chrono>

#include <caf/stateful_actor.hpp>

namespace vast {

class path;

namespace system {

struct profiler_state {
  static inline const char* name = "profiler";
};

/// Profiles CPU and heap usage via gperftools.
/// @param self The actor handle.
/// @param dir The directory where to write profiler output to.
/// @param secs The number of seconds between subsequent measurements.
caf::behavior profiler(caf::stateful_actor<profiler_state>* self, path dir,
                       std::chrono::seconds secs);

} // namespace system
} // namespace vast

#endif
