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

/// The PARTITION CLIENT actor interface.
// The client sends an expression to the partition and receives several sets
// of ids followed by a final `atom::done` which as sent as response to the
// expression. This interface provides the callback for the middle part of
// this sequence.
using partition_client_actor = caf::typed_actor<caf::reacts_to<ids>>;

} // namespace vast::system
