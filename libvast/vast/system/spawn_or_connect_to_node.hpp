//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/scope_linked.hpp"
#include "vast/system/actors.hpp"

#include <variant>

namespace vast::system {

/// Either spawns a new VAST node or connects to a server, depending on the
/// configuration.
std::variant<caf::error, node_actor, scope_linked<node_actor>>
spawn_or_connect_to_node(caf::scoped_actor& self, const caf::settings& opts,
                         const caf::settings& node_opts);

} // namespace vast::system
