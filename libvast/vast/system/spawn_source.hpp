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
#include "vast/system/import_command.hpp"
#include "vast/system/node.hpp"
#include "vast/system/spawn_arguments.hpp"

namespace vast::system {

/// Tries to spawn a new SOURCE for the specified format.
/// @tparam Reader the format-specific reader
/// @param self Points to the parent actor.
/// @param args Configures the new actor.
/// @returns a handle to the spawned actor on success, an error otherwise
template <class Reader, class Defaults = typename Reader::defaults>
maybe_actor spawn_source(node_actor* self, spawn_arguments& args) {
  VAST_TRACE(VAST_ARG("node", self), VAST_ARG(args));
  auto& st = self->state;
  auto& options = args.inv.options;
  // Bail out early for bogus invocations.
  if (caf::get_or(options, "system.node", false))
    return make_error(ec::invalid_configuration,
                      "unable to spawn a remote source when spawning a node "
                      "locally instead of connecting to one; please unset "
                      "the option system.node");
  auto accountant = accountant_type{};
  if (auto a = self->system().registry().get(atom::accountant_v))
    accountant = caf::actor_cast<accountant_type>(a);
  auto src = configure_source<Reader, Defaults, caf::detached>(
    self, self->system(), args.inv, std::move(accountant), st.type_registry,
    st.importer);
  if (!src)
    return src.error();
  (*src)->attach_functor([=](const caf::error& reason) {
    if (!reason || reason == caf::exit_reason::user_shutdown)
      VAST_INFO(src, "source shut down");
    else
      VAST_WARNING(src, "source shut down with error:", reason);
  });
  return src;
}

} // namespace vast::system
