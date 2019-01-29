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

#include <caf/fwd.hpp>

#include "vast/aliases.hpp"
#include "vast/system/fwd.hpp"

namespace vast::system {

/// Tries to spawn a new SOURCE for the PCAP format.
/// @param self Points to the parent actor.
/// @param args Configures the new actor.
/// @returns a handle to the spawned actor on success, an error otherwise
maybe_actor spawn_pcap_source(caf::local_actor* self, spawn_arguments& args);

/// Tries to spawn a new SOURCE for generating random data.
/// @param self Points to the parent actor.
/// @param args Configures the new actor.
/// @returns a handle to the spawned actor on success, an error otherwise
maybe_actor spawn_test_source(caf::local_actor* self, spawn_arguments& args);

/// Tries to spawn a new SOURCE for the Zeek format.
/// @param self Points to the parent actor.
/// @param args Configures the new actor.
/// @returns a handle to the spawned actor on success, an error otherwise
maybe_actor spawn_zeek_source(caf::local_actor* self, spawn_arguments& args);

/// Tries to spawn a new SOURCE for the BGPdump format.
/// @param self Points to the parent actor.
/// @param args Configures the new actor.
/// @returns a handle to the spawned actor on success, an error otherwise
maybe_actor spawn_bgpdump_source(caf::local_actor* self, spawn_arguments& args);

/// Tries to spawn a new SOURCE for the MRT format.
/// @param self Points to the parent actor.
/// @param args Configures the new actor.
/// @returns a handle to the spawned actor on success, an error otherwise
maybe_actor spawn_mrt_source(caf::local_actor* self, spawn_arguments& args);

} // namespace vast::system
