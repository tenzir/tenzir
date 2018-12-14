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

#include <vector>

#include "vast/system/index.hpp"
#include "vast/table_slice.hpp"

#include "vast/test/fixtures/actor_system_and_events.hpp"

namespace fixtures {

/// A fixture with a dummy INDEX actor.
struct dummy_index : deterministic_actor_system_and_events {
  // -- member types -----------------------------------------------------------

  struct dummy_indexer_state {
    std::vector<vast::table_slice_ptr> buf;
  };

  // -- constructors, destructors, and assignment operators --------------------

  dummy_index();

  ~dummy_index() override;

  // -- convenience functions --------------------------------------------------

  auto& indexer_buf(caf::actor& hdl) {
    return deref<caf::stateful_actor<dummy_indexer_state>>(hdl).state.buf;
  }

  /// Runs `f` inside the dummy INDEX actor.
  void run_in_index(std::function<void()> f);

  // -- member variables -------------------------------------------------------

  /// Actor handle to our dummy.
  caf::actor idx_handle;

  vast::system::index_state* idx_state;
};

} // namespace fixtures
