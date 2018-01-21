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

#ifndef VAST_SYSTEM_ACCOUNTANT_HPP
#define VAST_SYSTEM_ACCOUNTANT_HPP

#include <cstdint>
#include <fstream>
#include <string>

#include <caf/typed_actor.hpp>

#include "vast/filesystem.hpp"
#include "vast/time.hpp"

#include "vast/system/atoms.hpp"

namespace vast::system {

struct accountant_state {
  using stopwatch = std::chrono::steady_clock;
  std::ofstream file;
  const char* name = "accountant";
};


using accountant_type =
  caf::typed_actor<
    caf::reacts_to<shutdown_atom>,
    caf::reacts_to<flush_atom>,
    caf::reacts_to<std::string, std::string>,
    caf::reacts_to<std::string, timespan>,
    caf::reacts_to<std::string, timestamp>,
    caf::reacts_to<std::string, int64_t>,
    caf::reacts_to<std::string, uint64_t>,
    caf::reacts_to<std::string, double>
  >;

/// Accumulates various performance metrics in a key-value format and writes
/// them to a log file.
/// @param self The actor handle.
/// @param filename The path of the file containing the accounting details.
accountant_type::behavior_type
accountant(accountant_type::stateful_pointer<accountant_state> self,
           path const& filename);

} // namespace vast::system

#endif
