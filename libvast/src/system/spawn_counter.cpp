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

#include "vast/system/spawn_counter.hpp"

#include "vast/defaults.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/system/archive.hpp"
#include "vast/system/counter.hpp"
#include "vast/system/node.hpp"
#include "vast/system/node_control.hpp"
#include "vast/system/spawn_arguments.hpp"

#include <caf/actor.hpp>
#include <caf/expected.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/send.hpp>
#include <caf/settings.hpp>

namespace vast::system {

maybe_actor
spawn_counter(system::node_actor* self, system::spawn_arguments& args) {
  VAST_TRACE(VAST_ARG(args));
  // Parse given expression.
  auto expr = system::normalized_and_validated(args);
  if (!expr)
    return expr.error();
  auto [index, archive]
    = self->state.registry.find_by_label("index", "archive");
  if (!index)
    return make_error(ec::missing_component, "index");
  if (!archive)
    return make_error(ec::missing_component, "archive");
  auto estimate = caf::get_or(args.inv.options, "count.estimate", false);
  return self->spawn(counter, std::move(*expr), index,
                     caf::actor_cast<archive_type>(archive), estimate);
}

} // namespace vast::system
