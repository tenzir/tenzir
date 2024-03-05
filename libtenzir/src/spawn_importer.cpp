//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/spawn_importer.hpp"

#include "tenzir/actors.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/importer.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/node.hpp"
#include "tenzir/spawn_arguments.hpp"
#include "tenzir/uuid.hpp"

#include <caf/settings.hpp>
#include <caf/typed_event_based_actor.hpp>

namespace tenzir {

caf::expected<caf::actor>
spawn_importer(node_actor::stateful_pointer<node_state> self,
               spawn_arguments& args) {
  if (!args.empty())
    return unexpected_arguments(args);
  // FIXME: Notify exporters with a continuous query.
  auto [index, accountant]
    = self->state.registry.find<index_actor, accountant_actor>();
  auto store_backend = std::string{defaults::store_backend};
  if (!index)
    return caf::make_error(ec::missing_component, "index");
  auto handle = self->spawn(importer, args.dir / args.label, index, accountant);
  TENZIR_VERBOSE("{} spawned the importer", *self);
  for (auto& source : self->state.registry.find_by_type("source")) {
    TENZIR_DEBUG("{} connects source to new importer", *self);
    self->anon_send(source, atom::sink_v, caf::actor_cast<caf::actor>(handle));
  }
  return caf::actor_cast<caf::actor>(handle);
}

} // namespace tenzir
