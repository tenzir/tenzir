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

namespace vast {

/// Spawns a new VAST node.
/// `self` should be equipped to handle (atom::signal, int)
/// messages to orchestrate a graceful termination if it runs
/// a receive-while/until loop after this call.
caf::expected<scope_linked<node_actor>>
spawn_node(caf::scoped_actor& self, const caf::settings& opts);

} // namespace vast
