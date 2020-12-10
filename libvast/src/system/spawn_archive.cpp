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

#include "vast/system/spawn_archive.hpp"

#include "vast/defaults.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/si_literals.hpp"
#include "vast/system/accountant_actor.hpp"
#include "vast/system/archive_actor.hpp"
#include "vast/system/node.hpp"
#include "vast/system/spawn_arguments.hpp"

#include <caf/actor.hpp>
#include <caf/actor_cast.hpp>
#include <caf/config_value.hpp>
#include <caf/expected.hpp>
#include <caf/local_actor.hpp>
#include <caf/settings.hpp>

using namespace vast::binary_byte_literals;

namespace vast::system {

maybe_actor spawn_archive(node_actor* self, spawn_arguments& args) {
  namespace sd = vast::defaults::system;
  if (!args.empty())
    return unexpected_arguments(args);
  auto segments = get_or(args.inv.options, "vast.segments", sd::segments);
  auto max_segment_size
    = 1_MiB
      * get_or(args.inv.options, "vast.max-segment-size", sd::max_segment_size);
  auto handle
    = self->spawn(archive, args.dir / args.label, segments, max_segment_size);
  VAST_VERBOSE(self, "spawned the archive");
  if (auto accountant = self->state.registry.find_by_label("accountant"))
    self->send(handle, caf::actor_cast<accountant_actor>(accountant));
  return caf::actor_cast<caf::actor>(handle);
}

} // namespace vast::system
