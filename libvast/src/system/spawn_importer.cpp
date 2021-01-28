/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/system/spawn_importer.hpp"

#include "vast/defaults.hpp"
#include "vast/detail/assert.hpp"
#include "vast/logger.hpp"
#include "vast/system/importer.hpp"
#include "vast/system/node.hpp"
#include "vast/system/spawn_arguments.hpp"
#include "vast/uuid.hpp"

#include <caf/settings.hpp>

namespace vast::system {

maybe_actor spawn_importer(node_actor* self, spawn_arguments& args) {
  if (!args.empty())
    return unexpected_arguments(args);
  // FIXME: Notify exporters with a continuous query.
  auto [archive, index, type_registry]
    = self->state.registry.find_by_label("archive", "index", "type-registry");
  if (!archive)
    return caf::make_error(ec::missing_component, "archive");
  if (!index)
    return caf::make_error(ec::missing_component, "index");
  if (!type_registry)
    return caf::make_error(ec::missing_component, "type-registry");
  auto handle
    = self->spawn(importer, args.dir / args.label,
                  caf::actor_cast<archive_actor>(archive),
                  caf::actor_cast<index_actor>(index),
                  caf::actor_cast<type_registry_actor>(type_registry));
  VAST_LOG_SPD_VERBOSE("{} spawned the importer", detail::id_or_name(self));
  if (auto accountant = self->state.registry.find_by_label("accountant")) {
    self->send(handle, atom::telemetry_v);
    self->send(handle, caf::actor_cast<accountant_actor>(accountant));
  } else if (auto logger = caf::logger::current_logger();
             logger && logger->console_verbosity() >= VAST_LOG_LEVEL_VERBOSE) {
    // Initiate periodic rate logging.
    // TODO: Implement live-reloading of the importer configuration.
    self->send(handle, atom::telemetry_v);
  }
  for (auto& source : self->state.registry.find_by_type("source")) {
    VAST_LOG_SPD_DEBUG("{} connects source to new importer",
                       detail::id_or_name(self));
    self->send(source, atom::sink_v, caf::actor_cast<caf::actor>(handle));
  }
  return caf::actor_cast<caf::actor>(handle);
}

} // namespace vast::system
