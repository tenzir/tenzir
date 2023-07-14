//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/actors.hpp"
#include "tenzir/data.hpp"
#include "tenzir/scope_linked.hpp"

#include <variant>

namespace tenzir {

/// Either spawns a new Tenzir node or connects to a server, depending on the
/// configuration.
/// `self` should be equipped to handle (atom::signal, int)
/// messages to orchestrate a graceful termination if it runs
/// a receive-while/until loop after this call.
std::variant<caf::error, node_actor, scope_linked<node_actor>>
spawn_or_connect_to_node(caf::scoped_actor& self, const caf::settings& opts,
                         const caf::settings& node_opts);

} // namespace tenzir
