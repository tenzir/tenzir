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

#pragma once

#include "vast/fwd.hpp"

#include "vast/logger.hpp"
#include "vast/system/actors.hpp"
#include "vast/system/make_source.hpp"
#include "vast/system/node.hpp"
#include "vast/system/spawn_arguments.hpp"

#include <caf/typed_event_based_actor.hpp>

namespace vast::system {

/// Tries to spawn a new SOURCE for the specified format.
/// @tparam Reader the format-specific reader
/// @param self Points to the parent actor.
/// @param args Configures the new actor.
/// @returns a handle to the spawned actor on success, an error otherwise
template <class Reader, class Defaults = typename Reader::defaults>
caf::expected<caf::actor>
spawn_source(node_actor::stateful_pointer<node_state> self,
             spawn_arguments& args) {
  VAST_TRACE_SCOPE("{} {}", VAST_ARG("node", self), VAST_ARG(args));
  auto& options = args.inv.options;
  // Bail out early for bogus invocations.
  if (caf::get_or(options, "vast.node", false))
    return caf::make_error(ec::invalid_configuration,
                           "unable to spawn a remote source when spawning a "
                           "node locally instead of connecting to one; please "
                           "unset the option vast.node");
  auto [accountant, importer, type_registry]
    = self->state.registry
        .find<accountant_actor, importer_actor, type_registry_actor>();
  if (!importer)
    return caf::make_error(ec::missing_component, "importer");
  if (!type_registry)
    return caf::make_error(ec::missing_component, "type-registry");
  auto src_result = make_source<Reader, Defaults, caf::detached>(
    self, self->system(), args.inv, accountant, type_registry, importer);
  if (!src_result)
    return src_result.error();
  auto src = std::move(src_result->src);
  auto name = std::move(src_result->name);
  VAST_INFO("{} spawned a {} source", self, name);
  src->attach_functor([=](const caf::error& reason) {
    if (!reason || reason == caf::exit_reason::user_shutdown)
      VAST_INFO("{} source shut down", detail::pretty_type_name(name));
    else
      VAST_WARN("{} source shut down with error: {}",
                detail::pretty_type_name(name), reason);
  });
  return src;
}

} // namespace vast::system
