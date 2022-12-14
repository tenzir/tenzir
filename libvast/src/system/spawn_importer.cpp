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
#include "vast/system/actors.hpp"
#include "vast/system/importer.hpp"
#include "vast/system/make_pipelines.hpp"
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
  auto [archive, index, accountant]
    = self->state.registry.find<archive_actor, index_actor, accountant_actor>();
  auto store_backend
    = caf::get_or(args.inv.options, "vast.store-backend",
                  std::string{defaults::system::store_backend});
  auto partition_local_stores = store_backend != "archive";
  auto pipelines
    = make_pipelines(pipelines_location::server_import, args.inv.options);
  if (!pipelines)
    return pipelines.error();
  // Don't send incoming data to the global archive if we use partition-local
  // stores.
  if (partition_local_stores)
    archive = nullptr;
  if (!archive && !partition_local_stores)
    return caf::make_error(ec::missing_component, "archive");
  if (!index)
    return caf::make_error(ec::missing_component, "index");
  auto handle = self->spawn(importer, args.dir / args.label, archive, index,
                            std::move(*pipelines));
  VAST_VERBOSE("{} spawned the importer", *self);
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
    VAST_DEBUG("{} connects source to new importer", *self);
    self->anon_send(source, atom::sink_v, caf::actor_cast<caf::actor>(handle));
  }
  return caf::actor_cast<caf::actor>(handle);
}

} // namespace vast::system
