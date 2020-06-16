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
#include "vast/detail/unbox_var.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/system/archive.hpp"
#include "vast/system/counter.hpp"
#include "vast/system/node.hpp"
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
  VAST_UNBOX_VAR(expr, system::normalized_and_validated(args));
  // Get INDEX and ARCHIVE.
  caf::error err;
  caf::actor index;
  system::archive_type archive;
  caf::scoped_actor blocking{self->system()};
  blocking->request(self->state.tracker, caf::infinite, atom::get::value)
    .receive(
      [&](system::registry& reg) {
        VAST_DEBUG(self, "looks for index and archive");
        auto by_name = [&](std::string key) -> caf::actor {
          auto& local = reg.components[self->state.name];
          auto [first, last] = local.equal_range(key);
          if (first == last)
            err = make_error(ec::invalid_configuration, "missing actor", key);
          else if (std::distance(first, last) > 1)
            err = make_error(ec::invalid_configuration,
                             "too many actors for label", key);
          else
            return first->second.actor;
          return nullptr;
        };
        index = by_name("index");
        archive = caf::actor_cast<system::archive_type>(by_name("archive"));
      },
      [&](caf::error& tracker_error) { err = std::move(tracker_error); });
  if (err)
    return err;
  VAST_ASSERT(index != nullptr);
  VAST_ASSERT(archive != nullptr);
  return self->spawn(counter, std::move(expr), index, archive,
                     caf::get_or(args.inv.options, "count.estimate", false));
}

} // namespace vast::system
