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

#include <caf/typed_event_based_actor.hpp>

namespace vast::system {

/// The TYPE REGISTRY actor interface.
using type_registry_actor = caf::typed_actor<
  // The internal telemetry loop of the TYPE REGISTRY.
  caf::reacts_to<atom::telemetry>,
  // Hooks into the table slice stream.
  caf::replies_to<caf::stream<table_slice>>::with< //
    caf::inbound_stream_slot<table_slice>>,
  // Registers the given type.
  caf::reacts_to<atom::put, vast::type>,
  // Registers all types in the given schema.
  caf::reacts_to<atom::put, vast::schema>,
  // Retrieves all known types.
  caf::replies_to<atom::get>::with<type_set>,
  // Registers the given taxonomies.
  caf::reacts_to<atom::put, taxonomies>,
  // Retrieves the known taxonomies.
  caf::replies_to<atom::get, atom::taxonomies>::with< //
    taxonomies>,
  // Loads the taxonomies on disk.
  caf::replies_to<atom::load>::with< //
    atom::ok>,
  // Resolves an expression in terms of the known taxonomies.
  caf::replies_to<atom::resolve, expression>::with< //
    expression>,
  // Registers the TYPE REGISTRY with the ACCOUNTANT.
  caf::reacts_to<accountant_actor>>
  // Conform to the procotol of the STATUS CLIENT actor.
  ::extend_with<status_client_actor>;

} // namespace vast::system
