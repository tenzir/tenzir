//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/actors.hpp>
#include <tenzir/metric_handler.hpp>
#include <tenzir/tql2/ast.hpp>

namespace tenzir {

// export_mode and export_bridge_actor are defined in actors.hpp

TENZIR_ENUM(event_source, unpersisted, live, retro);

auto spawn_and_link_export_bridge(
  caf::scheduled_actor& parent, expression expr, export_mode mode,
  filesystem_actor filesystem, metric_handler metrics_handler,
  std::unique_ptr<diagnostic_handler> diagnostics_handler)
  -> export_bridge_actor;

/// Spawn an export bridge without linking to a parent actor.
/// This is used by the new executor which doesn't run operators as actors.
auto spawn_export_bridge(caf::actor_system& sys, expression expr,
                         export_mode mode, filesystem_actor filesystem,
                         std::unique_ptr<diagnostic_handler> diagnostics_handler)
  -> export_bridge_actor;

} // namespace tenzir
