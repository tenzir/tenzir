// SPDX-FileCopyrightText: (c) 2018 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/scope_linked.hpp"
#include "vast/system/actors.hpp"

namespace vast::system {

/// Spawns a new VAST node.
caf::expected<scope_linked<node_actor>>
spawn_node(caf::scoped_actor& self, const caf::settings& opts);

} // namespace vast::system
