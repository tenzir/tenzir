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
  auto fs = caf::actor_cast<filesystem_type>(
    self->state.registry.find_by_label("filesystem"));
  if (!fs)
    return make_error(ec::lookup_error, "couldnt find filesystem actor");
  namespace sd = vast::defaults::system;
  auto idx
    = self->spawn(index, fs, args.dir / args.label,
                  opt("system.max-partition-size", sd::max_partition_size),
                  opt("system.in-mem-partitions", sd::max_in_mem_partitions),
                  opt("system.taste-partitions", sd::taste_partitions),
                  opt("system.query-supervisors", sd::num_query_supervisors),
                  opt("system.disable-recoverability", false));
  if (auto accountant = self->state.registry.find_by_label("accountant"))
    self->send(idx, caf::actor_cast<accountant_type>(accountant));
  return idx;
}

} // namespace vast::system
