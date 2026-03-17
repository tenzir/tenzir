//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/actors.hpp"
#include "tenzir/scope_linked.hpp"

namespace tenzir {

/// Spawns a new Tenzir node.
/// `self` should be equipped to handle (atom::signal, int)
/// messages to orchestrate a graceful termination if it runs
/// a receive-while/until loop after this call.
auto spawn_node(caf::scoped_actor& self)
  -> caf::expected<scope_linked<node_actor>>;

} // namespace tenzir
