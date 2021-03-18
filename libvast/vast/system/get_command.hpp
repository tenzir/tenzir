// SPDX-FileCopyrightText: (c) 2020 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/aliases.hpp"

namespace vast::system {

/// Gets events by ID.
caf::message get_command(const invocation& invocation, caf::actor_system& sys);

} // namespace vast::system
