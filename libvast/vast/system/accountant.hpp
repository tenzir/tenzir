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

#include "vast/fwd.hpp"
#include "vast/status.hpp"
#include "vast/time.hpp"

#include <caf/typed_actor.hpp>

namespace vast::system {

// Forward declarations
struct accountant_config;
struct accountant_state;

struct accountant_state_deleter {
  void operator()(accountant_state* st);
};

using accountant_state_ptr
  = std::unique_ptr<accountant_state, accountant_state_deleter>;

// clang-format off
/// @relates accountant
using accountant_type = caf::typed_actor<
  caf::replies_to<atom::config, accountant_config>::with<atom::ok>,
  caf::reacts_to<atom::announce, std::string>,
  caf::reacts_to<std::string, duration>,
  caf::reacts_to<std::string, time>,
  caf::reacts_to<std::string, int64_t>,
  caf::reacts_to<std::string, uint64_t>,
  caf::reacts_to<std::string, double>,
  caf::reacts_to<report>,
  caf::reacts_to<performance_report>,
  caf::replies_to<atom::status, status_verbosity>::with<caf::dictionary<caf::config_value>>,
  caf::reacts_to<atom::telemetry>>;
// clang-format on

/// Accumulates various performance metrics in a key-value format and writes
/// them to VAST table slices.
/// @param self The actor handle.
accountant_type::behavior_type
accountant(accountant_type::stateful_pointer<accountant_state_ptr> self,
           accountant_config cfg);

} // namespace vast::system
