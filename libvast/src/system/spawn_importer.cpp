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

#include <caf/actor.hpp>
#include <caf/actor_system_config.hpp>
#include <caf/expected.hpp>
#include <caf/settings.hpp>

namespace vast::system {

maybe_actor spawn_importer(node_actor* self, spawn_arguments& args) {
  if (!args.empty())
    return unexpected_arguments(args);
  // FIXME: Notify exporters with a continuous query.
  auto [archive, index, type_registry]
    = self->state.registry.find_by_label("archive", "index", "type-registry");
  if (!archive)
    return make_error(ec::missing_component, "archive");
  if (!index)
    return make_error(ec::missing_component, "index");
  if (!type_registry)
    return make_error(ec::missing_component, "type-registry");
  auto imp = self->spawn(importer, args.dir / args.label,
                         caf::actor_cast<archive_type>(archive), index,
                         caf::actor_cast<type_registry_type>(type_registry));
  if (auto accountant = self->state.registry.find_by_label("accountant"))
    self->send(imp, caf::actor_cast<accountant_type>(accountant));
  for (auto& a : self->state.registry.find_by_type("source")) {
    VAST_DEBUG(self, "connects source to new importer");
    self->send(a, atom::sink_v, imp);
  }
  return imp;
}

} // namespace vast::system
