//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/spawn_importer.hpp"

#include "vast/defaults.hpp"
#include "vast/detail/assert.hpp"
#include "vast/logger.hpp"
#include "vast/qualified_record_field.hpp"
#include "vast/system/actors.hpp"
#include "vast/system/importer.hpp"
#include "vast/system/make_transforms.hpp"
#include "vast/system/node.hpp"
#include "vast/system/spawn_arguments.hpp"
#include "vast/uuid.hpp"

#include <caf/settings.hpp>
#include <caf/typed_event_based_actor.hpp>

namespace vast::system {

caf::expected<caf::actor>
spawn_importer(node_actor::stateful_pointer<node_state> self,
               spawn_arguments& args) {
  if (!args.empty())
    return unexpected_arguments(args);
  // FIXME: Notify exporters with a continuous query.
  auto [archive, index, type_registry, accountant]
    = self->state.registry.find<archive_actor, index_actor, type_registry_actor,
                                accountant_actor>();
  auto partition_local_stores
    = caf::get_or(args.inv.options, "vast.partition-local-stores",
                  defaults::system::partition_local_stores);
  auto transforms
    = make_transforms(transforms_location::server_import, args.inv.options);
  if (!transforms)
    return transforms.error();
  // Don't send incoming data to the global archive if we use partition-local
  // stores.
  if (partition_local_stores)
    archive = nullptr;
  if (!archive && !partition_local_stores)
    return caf::make_error(ec::missing_component, "archive");
  if (!index)
    return caf::make_error(ec::missing_component, "index");
  if (!type_registry)
    return caf::make_error(ec::missing_component, "type-registry");
  auto handle = self->spawn(importer, args.dir / args.label, archive, index,
                            type_registry, std::move(*transforms));
  VAST_VERBOSE("{} spawned the importer", self);
  if (accountant) {
    self->send(handle, atom::telemetry_v);
    self->send(handle, accountant);
  } else if (auto logger = caf::logger::current_logger();
             logger && logger->console_verbosity() >= VAST_LOG_LEVEL_VERBOSE) {
    // Initiate periodic rate logging.
    // TODO: Implement live-reloading of the importer configuration.
    self->send(handle, atom::telemetry_v);
  }
  for (auto& source : self->state.registry.find_by_type("source")) {
    VAST_DEBUG("{} connects source to new importer", self);
    self->anon_send(source, atom::sink_v, caf::actor_cast<caf::actor>(handle));
  }
  return caf::actor_cast<caf::actor>(handle);
}

} // namespace vast::system
