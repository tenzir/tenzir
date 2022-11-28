//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/spawn_type_registry.hpp"

#include "vast/defaults.hpp"
#include "vast/logger.hpp"
#include "vast/system/catalog.hpp"
#include "vast/system/node.hpp"
#include "vast/system/spawn_arguments.hpp"

#include <caf/typed_event_based_actor.hpp>

namespace vast::system {

caf::expected<caf::actor>
spawn_type_registry(node_actor::stateful_pointer<node_state> self,
                    spawn_arguments& args) {
  if (!args.empty())
    return unexpected_arguments(args);
  auto handle = self->spawn(type_registry, args.dir / args.label);
  self->request(handle, caf::infinite, atom::load_v)
    .await([](atom::ok) {},
           [](caf::error& err) {
             VAST_WARN("type-registry failed to load taxonomy "
                       "definitions: {}",
                       std::move(err));
           });
  VAST_VERBOSE("{} spawned the type-registry", *self);
  if (auto [accountant] = self->state.registry.find<accountant_actor>();
      accountant)
    self->send(handle, atom::set_v, accountant);
  return caf::actor_cast<caf::actor>(handle);
}

} // namespace vast::system
