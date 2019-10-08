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

#include <functional>

#include "vast/command.hpp"

namespace vast::system {

/// Callback for adding additional application logic to `start_command_impl`.
/// @relates start_command_impl
using start_command_extra_steps = std::function<caf::error(
  caf::scoped_actor& self, const caf::settings& options, const caf::actor&)>;

/// Extensible base implementation for the *start* command that allows
/// users to add additional application logic.
/// @param extra_steps Function that adds additional application logic after
///                    the node is connected and before the command enters its
///                    loop to wait for CTRL+C or system shutdown.
/// @param invocation Invocation object that dispatches to this function.
/// @param sys The hosting CAF actor system.
/// @returns An non-default error on if the extra steps fail and
///          `start_command_impl` needs to stop running, `caf::none` otherwise.
/// @relates start_command
caf::message start_command_impl(start_command_extra_steps extra_steps,
                                const command::invocation& invocation,
                                caf::actor_system& sys);

/// Default implementation for the *start* command.
/// @param invocation Invocation object that dispatches to this function.
/// @param sys The hosting CAF actor system.
/// @returns An error on invalid arguments or when unable to connect to the
///          remote node, an empty message otherwise.
/// @relates application
caf::message
start_command(const command::invocation& invocation, caf::actor_system& sys);

} // namespace vast::system
