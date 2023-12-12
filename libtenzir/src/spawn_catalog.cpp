//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/spawn_catalog.hpp"

#include "tenzir/catalog.hpp"
#include "tenzir/node.hpp"
#include "tenzir/query_context.hpp"
#include "tenzir/spawn_arguments.hpp"

#include <caf/typed_event_based_actor.hpp>

namespace tenzir {

caf::expected<caf::actor>
spawn_catalog(node_actor::stateful_pointer<node_state> self,
              spawn_arguments& args) {
  auto [accountant] = self->state.registry.find<accountant_actor>();
  auto detached = caf::get_or(args.inv.options, "tenzir.detach-components",
                              defaults::detach_components);
  auto handle = detached ? self->spawn<caf::detached>(catalog, accountant)
                         : self->spawn(catalog, accountant);
  TENZIR_VERBOSE("{} spawned the catalog", *self);
  return caf::actor_cast<caf::actor>(handle);
}

} // namespace tenzir
