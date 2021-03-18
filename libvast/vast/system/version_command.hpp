// SPDX-FileCopyrightText: (c) 2019 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/aliases.hpp"

namespace vast::system {

/// Prints the version information to stdout.
void print_version(const record& extra_content = {});

/// Displays the software version to the user.
caf::message version_command(const invocation& inv, caf::actor_system& sys);

} // namespace vast::system
