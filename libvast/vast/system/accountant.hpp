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
#include "vast/system/report.hpp"
#include "vast/time.hpp"

#include <caf/broadcast_downstream_manager.hpp>
#include <caf/fwd.hpp>
#include <caf/typed_actor.hpp>

#include <cstdint>
#include <fstream>
#include <queue>
#include <string>

namespace vast::system {

// clang-format off
/// @relates accountant
using accountant_type = caf::typed_actor<
  caf::reacts_to<atom::announce, std::string>,
  caf::reacts_to<std::string, duration>,
  caf::reacts_to<std::string, time>,
  caf::reacts_to<std::string, int64_t>,
  caf::reacts_to<std::string, uint64_t>,
  caf::reacts_to<std::string, double>,
  caf::reacts_to<report>,
  caf::reacts_to<performance_report>,
  caf::replies_to<atom::status>::with<caf::dictionary<caf::config_value>>,
  caf::reacts_to<atom::telemetry>>;
// clang-format on

/// @relates accountant
struct accountant_state {
  // -- member types -----------------------------------------------------------

  using downstream_manager = caf::broadcast_downstream_manager<table_slice_ptr>;

  // -- constructors, destructors, and assignment operators --------------------

  accountant_state(accountant_type::stateful_base<accountant_state>* self);

  // -- functions --------------------------------------------------------------

  /// Prints information about the current load to the INFO log.
  void command_line_heartbeat();

  // -- member variables -------------------------------------------------------

  /// Stores the names of known actors to fill into the actor_name column.
  std::unordered_map<caf::actor_id, std::string> actor_map;

  /// Accumulates the importer throughput until the next heartbeat.
  measurement accumulator;

  /// The maximum size of generated slices.
  size_t slice_size;

  /// Stores the builder instance.
  table_slice_builder_ptr builder;

  /// Buffers table_slices, acting as a adaptor between the push based
  /// ACCOUNTANT interface and the pull based stream to the IMPORTER.
  std::queue<table_slice_ptr> slice_buffer;

  /// Takes care of transmitting batches.
  caf::stream_source_ptr<downstream_manager> mgr;

  /// Pointer to the parent actor.
  accountant_type::stateful_pointer<accountant_state> self;

  /// Name of the ACCOUNTANT actor.
  static constexpr const char* name = "accountant";
};

/// Accumulates various performance metrics in a key-value format and writes
/// them to VAST table slices.
/// @param self The actor handle.
accountant_type::behavior_type
accountant(accountant_type::stateful_pointer<accountant_state> self);

} // namespace vast::system
