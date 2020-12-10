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

#include <caf/typed_event_based_actor.hpp>

namespace vast::system {

/// The INDEXER actor interface.
using indexer_actor = caf::typed_actor<
  // Returns the ids for the given predicate.
  caf::replies_to<curried_predicate>::with<ids>,
  // Requests the INDEXER to shut down.
  caf::reacts_to<atom::shutdown>>;

} // namespace vast::system
