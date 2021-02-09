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

#include "vast/aliases.hpp"

namespace vast::system {

/// Default implementation for the *start* command.
/// @param invocation Invocation object that dispatches to this function.
/// @param sys The hosting CAF actor system.
/// @returns An error on invalid arguments or when unable to connect to the
///          remote node, an empty message otherwise.
/// @relates application
caf::message start_command(const invocation& inv, caf::actor_system& sys);

} // namespace vast::system
