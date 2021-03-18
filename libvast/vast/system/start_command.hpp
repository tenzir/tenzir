// SPDX-FileCopyrightText: (c) 2018 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

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
