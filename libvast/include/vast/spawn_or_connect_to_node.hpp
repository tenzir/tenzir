//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/actors.hpp"
#include "vast/data.hpp"
#include "vast/scope_linked.hpp"

#include <variant>

namespace vast {

/// Either spawns a new VAST node or connects to a server, depending on the
/// configuration.
/// `self` should be equipped to handle (atom::signal, int)
/// messages to orchestrate a graceful termination if it runs
/// a receive-while/until loop after this call.
std::variant<caf::error, node_actor, scope_linked<node_actor>>
spawn_or_connect_to_node(caf::scoped_actor& self, const caf::settings& opts,
                         const caf::settings& node_opts);

} // namespace vast
