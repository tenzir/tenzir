//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/actors.hpp"
#include "tenzir/async/task.hpp"
#include "tenzir/diagnostics.hpp"

namespace tenzir {

/// Connects to the node, caching the result process-wide.
auto fetch_node(caf::actor_system& sys, diagnostic_handler& dh)
  -> Task<failure_or<node_actor>>;

} // namespace tenzir
