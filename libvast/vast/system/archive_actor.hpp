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
#include "vast/system/archive_client_actor.hpp"
#include "vast/system/status_client_actor.hpp"

#include <caf/typed_event_based_actor.hpp>

namespace vast::system {

using archive_actor = caf::typed_actor<
  // Hook into the table slice stream.
  caf::replies_to<caf::stream<table_slice>>::with< //
    caf::inbound_stream_slot<table_slice>>,
  // Register an exporter actor.
  // TODO: This should probably take an archive_client_actor.
  caf::reacts_to<atom::exporter, caf::actor>,
  // Registers the ARCHIVE with the ACCOUNTANT.
  caf::reacts_to<accountant_actor>,
  // Starts handling a query for the given ids.
  // TODO: This forwards to the second handler; this should probably be removed,
  // as it is not type safe.
  caf::reacts_to<ids>,
  // Starts handling a query for the given ids.
  caf::reacts_to<ids, archive_client_actor>,
  // Handles a query for the given ids, and sends the table slices back to the
  // ARCHIVE CLIENT.
  caf::reacts_to<ids, archive_client_actor, uint64_t>,
  // The internal telemetry loop of the ARCHIVE.
  caf::reacts_to<atom::telemetry>,
  // Erase the events with the given ids.
  caf::replies_to<atom::erase, ids>::with< //
    atom::done>>
  // Conform to the procotol of the STATUS CLIENT actor.
  ::extend_with<status_client_actor>;

} // namespace vast::system
