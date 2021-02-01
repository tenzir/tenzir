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

#include "vast/system/spawn_type_registry.hpp"

#include "vast/defaults.hpp"
#include "vast/logger.hpp"
#include "vast/system/node.hpp"
#include "vast/system/spawn_arguments.hpp"
#include "vast/system/type_registry.hpp"

namespace vast::system {

maybe_actor spawn_type_registry(node_actor* self, spawn_arguments& args) {
  if (!args.empty())
    return unexpected_arguments(args);
  auto handle = self->spawn(type_registry, args.dir / args.label);
  self->request(handle, defaults::system::initial_request_timeout, atom::load_v)
    .await([](atom::ok) {},
           [](caf::error& err) {
             VAST_WARN("type-registry failed to load taxonomy "
                       "definitions: {}",
                       render(std::move(err)));
           });
  VAST_VERBOSE("{} spawned the type-registry", self);
  if (auto [accountant] = self->state.registry.find<accountant_actor>();
      accountant)
    self->send(handle, caf::actor_cast<accountant_actor>(accountant));
  return caf::actor_cast<caf::actor>(handle);
}

} // namespace vast::system
