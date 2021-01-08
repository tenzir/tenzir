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
#include "vast/system/archive_actor.hpp"
#include "vast/system/archive_client_actor.hpp"
#include "vast/system/index_actor.hpp"
#include "vast/system/index_client_actor.hpp"
#include "vast/system/status_client_actor.hpp"

#include <caf/typed_event_based_actor.hpp>

namespace vast::system {

using exporter_actor = caf::typed_actor<
  // Request extraction of all events.
  caf::reacts_to<atom::extract>,
  // Request extraction of the given number of events.
  caf::reacts_to<atom::extract, uint64_t>,
  // Register the ACCOUNTANT actor.
  caf::reacts_to<accountant_actor>,
  // Register the ARCHIVE actor.
  caf::reacts_to<archive_actor>,
  // Register the INDEX actor.
  caf::reacts_to<index_actor>,
  // Register the SINK actor.
  caf::reacts_to<atom::sink, caf::actor>,
  // Register a list of IMPORTER actors.
  caf::reacts_to<atom::importer, std::vector<caf::actor>>,
  // Execute previously registered query.
  caf::reacts_to<atom::run>,
  // Register a STATISTICS SUBSCRIBER actor.
  caf::reacts_to<atom::statistics, caf::actor>,
  // Hook into the table slice stream.
  // TODO: This should probably be modeled as a IMPORTER CLIENT actor.
  caf::replies_to<caf::stream<table_slice>>::with< //
    caf::inbound_stream_slot<table_slice>>>
  // Conform to the protocol of the STATUS CLIENT actor.
  ::extend_with<status_client_actor>
  // Conform to the protocol of the INDEX CLIENT actor.
  ::extend_with<index_client_actor>
  // Conform to the protocol of the ARCHIVE CLIENT actor.
  ::extend_with<archive_client_actor>;

} // namespace vast::system
