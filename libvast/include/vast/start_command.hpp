//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/aliases.hpp"

namespace vast {

/// Default implementation for the *start* command.
/// @param invocation Invocation object that dispatches to this function.
/// @param sys The hosting CAF actor system.
/// @returns An error on invalid arguments or when unable to connect to the
///          remote node, an empty message otherwise.
/// @relates application
caf::message start_command(const invocation& inv, caf::actor_system& sys);

} // namespace vast
