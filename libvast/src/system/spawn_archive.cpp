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

#include <caf/actor.hpp>
#include <caf/actor_cast.hpp>
#include <caf/config_value.hpp>
#include <caf/expected.hpp>
#include <caf/local_actor.hpp>

#include "vast/defaults.hpp"
#include "vast/filesystem.hpp"
#include "vast/si_literals.hpp"
#include "vast/system/archive.hpp"
#include "vast/system/spawn_arguments.hpp"

using namespace vast::binary_byte_literals;

namespace vast::system {

maybe_actor spawn_archive(caf::local_actor* self, spawn_arguments& args) {
  namespace sd = vast::defaults::system;
  if (!args.empty())
    return unexpected_arguments(args);
  auto segments = get_or(args.options, "global.segments", sd::segments);
  auto mss = 1_MiB
             * get_or(args.options, "global.max-segment-size",
                      sd::max_segment_size);
  auto a = self->spawn(archive, args.dir / args.label, segments, mss);
  return caf::actor_cast<caf::actor>(a);
}

} // namespace vast::system
