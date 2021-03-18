// SPDX-FileCopyrightText: (c) 2018 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/aliases.hpp"

namespace vast::system {

/// Default implementation of a remote command.
/// @relates command
caf::message remote_command(const invocation& inv, caf::actor_system& sys);

} // namespace vast::system
