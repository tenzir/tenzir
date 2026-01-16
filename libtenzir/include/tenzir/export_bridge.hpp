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

struct export_bridge_traits {
  using signatures = caf::type_list<
    // Returns when a new table slice is available.
    auto(atom::get)->caf::result<table_slice>,
    // Insert a new table slice.
    auto(table_slice slice)->caf::result<void>>;
};

using export_bridge_actor = caf::typed_actor<export_bridge_traits>;

struct export_mode {
  bool retro = true;
  bool live = false;
  bool internal = false;
  uint64_t parallel = 3;

  export_mode() = default;

  export_mode(bool retro_, bool live_, bool internal_, uint64_t parallel_)
    : retro{retro_}, live{live_}, internal{internal_}, parallel{parallel_} {
    TENZIR_ASSERT(live or retro);
  }

  friend auto inspect(auto& f, export_mode& x) -> bool {
    return f.object(x).fields(f.field("retro", x.retro),
                              f.field("live", x.live),
                              f.field("internal", x.internal),
                              f.field("parallel", x.parallel));
  }
};

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
