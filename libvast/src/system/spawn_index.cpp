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

#include "vast/system/spawn_index.hpp"

#include "vast/defaults.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/system/index.hpp"
#include "vast/system/node.hpp"
#include "vast/system/spawn_arguments.hpp"

#include <caf/actor.hpp>
#include <caf/expected.hpp>
#include <caf/settings.hpp>

namespace vast::system {

maybe_actor spawn_index(node_actor* self, spawn_arguments& args) {
  if (!args.empty())
    return unexpected_arguments(args);
  auto opt = [&](caf::string_view key, auto default_value) {
    return get_or(args.inv.options, key, default_value);
  };
  auto filesystem = caf::actor_cast<filesystem_actor>(
    self->state.registry.find_by_label("filesystem"));
  if (!filesystem)
    return make_error(ec::lookup_error, "failed to find filesystem actor");
  namespace sd = vast::defaults::system;
  auto handle = self->spawn(
    index, filesystem, args.dir / args.label,
    opt("vast.max-partition-size", sd::max_partition_size),
    opt("vast.max-resident-partitions", sd::max_in_mem_partitions),
    opt("vast.max-taste-partitions", sd::taste_partitions),
    opt("vast.max-queries", sd::num_query_supervisors));
  VAST_VERBOSE(self, "spawned the index");
  if (auto accountant = self->state.registry.find_by_label("accountant"))
    self->send(handle, caf::actor_cast<accountant_actor>(accountant));
  return caf::actor_cast<caf::actor>(handle);
}

} // namespace vast::system
