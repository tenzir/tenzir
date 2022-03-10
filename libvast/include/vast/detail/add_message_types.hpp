//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <caf/fwd.hpp>

namespace vast::detail {

/// Adds VAST's message type to a CAF actor system configuration.
/// @param cfg The actor system configuration to add VAST's types to.
void add_message_types(caf::actor_system_config& cfg);

} // namespace vast::detail
