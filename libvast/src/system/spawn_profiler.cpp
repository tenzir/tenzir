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

#include "vast/system/spawn_profiler.hpp"

#include <caf/actor.hpp>
#include <caf/expected.hpp>

#include "vast/config.hpp"
#include "vast/detail/unbox_var.hpp"
#include "vast/error.hpp"
#include "vast/system/node.hpp"
#include "vast/system/spawn_arguments.hpp"

namespace vast::system {

maybe_actor spawn_profiler([[maybe_unused]] caf::local_actor* self,
                           [[maybe_unused]] spawn_arguments& args) {
#ifdef VAST_HAVE_GPERFTOOLS
  return make_error(ec::unspecified, "not compiled with gperftools");
#else // VAST_HAVE_GPERFTOOLS
  if (!args.empty())
    return unexpected_arguments(args);
  auto resolution = args.opt("global.resolution", size_t{1});
  auto secs = std::chrono::seconds(resolution);
  auto prof = self->spawn(profiler, args.dir / args.label, secs);
  if (args.opt("global.cpu", false))
    anon_send(prof, start_atom::value, cpu_atom::value);
  if (args.opt("global.heap", false))
    anon_send(prof, start_atom::value, heap_atom::value);
  return prof;
#endif // VAST_HAVE_GPERFTOOLS
}

} // namespace vast::system
