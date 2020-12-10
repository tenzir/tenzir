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

#include "vast/system/accountant_actor.hpp"

namespace vast::system {

// Forward declarations
struct accountant_state_impl;

struct accountant_state_deleter {
  void operator()(accountant_state_impl* ptr);
};

struct accountant_state
  : public std::unique_ptr<accountant_state_impl, accountant_state_deleter> {
  using unique_ptr::unique_ptr;

  // Name of the ACCOUNTANT actor.
  static constexpr const char* name = "accountant";
};

/// Accumulates various performance metrics in a key-value format and writes
/// them to VAST table slices.
/// @param self The actor handle.
accountant_actor::behavior_type
accountant(accountant_actor::stateful_pointer<accountant_state> self,
           accountant_config cfg);

} // namespace vast::system
