// SPDX-FileCopyrightText: (c) 2018 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <caf/fwd.hpp>

namespace vast::detail {

/// Adds VAST's message type to a CAF actor system configuration.
/// @param cfg The actor system configuration to add VAST's types to.
void add_message_types(caf::actor_system_config& cfg);

} // namespace vast::detail
