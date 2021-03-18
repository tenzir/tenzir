// SPDX-FileCopyrightText: (c) 2019 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/aliases.hpp"

namespace vast::system {

/// Starts a COUNTER actor and prints its result for a given query.
caf::message count_command(const invocation& inv, caf::actor_system& sys);

} // namespace vast::system
