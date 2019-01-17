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

maybe_actor spawn_index(caf::local_actor* self, spawn_arguments& args) {
  if (!args.empty())
    return unexpected_arguments(args);
  auto opt = [&](caf::string_view key, auto default_value) {
    return get_or(args.options, key, default_value);
  };
  namespace sd = vast::defaults::system;
  return self->spawn(index, args.dir / args.label,
                     opt("global.max-events", sd::max_partition_size),
                     opt("global.max-parts", sd::max_in_mem_partitions),
                     opt("global.taste-parts", sd::taste_partitions),
                     opt("global.max_queries", sd::num_query_supervisors));
}

} // namespace vast::system
