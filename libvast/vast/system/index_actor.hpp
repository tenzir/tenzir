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

#include "vast/meta_index.hpp"
#include "vast/system/accountant.hpp"
#include "vast/system/query_supervisor_master_actor.hpp"
#include "vast/system/status_client_actor.hpp"

#include <caf/typed_event_based_actor.hpp>

#include <memory>

namespace vast::system {

/// The INDEX actor interface.
using index_actor = caf::typed_actor<
  // Triggered when the INDEX finished querying a PARTITION.
  caf::reacts_to<atom::done, uuid>,
  // Hooks into the table slice stream.
  caf::replies_to<caf::stream<table_slice>>::with<
    caf::inbound_stream_slot<table_slice>>,
  // Registers the ARCHIVE with the ACCOUNTANT.
  caf::reacts_to<accountant_actor>,
  // Subscribes a FLUSH LISTENER to the INDEX.
  caf::reacts_to<atom::subscribe, atom::flush, wrapped_flush_listener>,
  // Evaluatates an expression.
  caf::reacts_to<expression>,
  // Queries PARTITION actors for a given query id.
  caf::reacts_to<uuid, uint32_t>,
  // Replaces the SYNOPSIS of the PARTITION witht he given partition id.
  caf::reacts_to<atom::replace, uuid, std::shared_ptr<partition_synopsis>>,
  // Erases the given events from the INDEX, and returns their ids.
  caf::replies_to<atom::erase, uuid>::with<ids>>
  // Conform to the protocol of the QUERY SUPERVISOR MASTER actor.
  ::extend_with<query_supervisor_master_actor>
  // Conform to the procol of the STATUS CLIENT actor.
  ::extend_with<status_client_actor>;

} // namespace vast::system
