// SPDX-FileCopyrightText: (c) 2020 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/aliases.hpp"

namespace vast::system {

/// Returns context around the results of a VAST query.
caf::message explore_command(const invocation& inv, caf::actor_system& sys);

} // namespace vast::system
