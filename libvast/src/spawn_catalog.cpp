//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/spawn_catalog.hpp"

#include "vast/catalog.hpp"
#include "vast/node.hpp"
#include "vast/query_context.hpp"
#include "vast/spawn_arguments.hpp"

#include <caf/typed_event_based_actor.hpp>

namespace vast {

caf::expected<caf::actor>
spawn_catalog(node_actor::stateful_pointer<node_state> self,
              spawn_arguments& args) {
  auto [accountant] = self->state.registry.find<accountant_actor>();
  auto detached = caf::get_or(args.inv.options, "tenzir.detach-components",
                              defaults::detach_components);
  auto handle
    = detached
        ? self->spawn<caf::detached>(catalog, accountant, args.dir / args.label)
        : self->spawn(catalog, accountant, args.dir / args.label);
  VAST_VERBOSE("{} spawned the catalog", *self);
  return caf::actor_cast<caf::actor>(handle);
}

} // namespace vast
