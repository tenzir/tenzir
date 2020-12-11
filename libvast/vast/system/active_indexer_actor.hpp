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

#include "vast/system/indexer_actor.hpp"

#include <caf/typed_event_based_actor.hpp>

namespace vast::system {

/// The ACTIVE INDEXER actor interface.
using active_indexer_actor = caf::typed_actor<
  // Hooks into the table slice column stream.
  caf::replies_to<caf::stream<table_slice_column>>::with<
    caf::inbound_stream_slot<table_slice_column>>,
  // Finalizes the ACTIVE INDEXER into a chunk, which containes an INDEXER.
  caf::replies_to<atom::snapshot>::with<chunk_ptr>>
  // Conform the the INDEXER ACTOR interface.
  ::extend_with<indexer_actor>;

} // namespace vast::system
