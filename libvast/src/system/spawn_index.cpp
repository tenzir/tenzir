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

#include <caf/actor.hpp>
#include <caf/expected.hpp>
#include <caf/settings.hpp>

#include "vast/defaults.hpp"
#include "vast/detail/unbox_var.hpp"
#include "vast/system/index.hpp"
#include "vast/system/node.hpp"
#include "vast/system/spawn_arguments.hpp"

namespace vast::system {

maybe_actor spawn_index(node_actor* self, spawn_arguments& args) {
  if (!args.empty())
    return unexpected_arguments(args);
  auto opt = [&](caf::string_view key, auto default_value) {
    return get_or(args.invocation.options, key, default_value);
  };
  namespace sd = vast::defaults::system;
  auto result = self->spawn(
    index, args.dir / args.label,
    opt("system.max-partition-size", sd::max_partition_size),
    opt("system.max-resident-partitions", sd::max_in_mem_partitions),
    opt("system.max-taste-partitions", sd::taste_partitions),
    opt("system.max-queries", sd::num_query_supervisors));
  self->state.index = result;
  return result;
}

} // namespace vast::system
