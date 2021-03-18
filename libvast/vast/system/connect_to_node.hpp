// SPDX-FileCopyrightText: (c) 2018 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/system/actors.hpp"

namespace vast::system {

/// Connects to a remote VAST server.
caf::expected<node_actor>
connect_to_node(caf::scoped_actor& self, const caf::settings& opts);

} // namespace vast::system
