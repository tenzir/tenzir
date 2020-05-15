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

#include "vast/system/spawn_explorer.hpp"

#include "vast/defaults.hpp"
#include "vast/filesystem.hpp"
#include "vast/si_literals.hpp"
#include "vast/system/explorer.hpp"
#include "vast/system/node.hpp"
#include "vast/system/spawn_arguments.hpp"

#include <caf/actor.hpp>
#include <caf/actor_cast.hpp>
#include <caf/config_value.hpp>
#include <caf/expected.hpp>
#include <caf/local_actor.hpp>
#include <caf/settings.hpp>

using namespace std::chrono_literals;

namespace vast::system {

maybe_actor spawn_explorer(node_actor* self, spawn_arguments& args) {
  if (!args.empty())
    return unexpected_arguments(args);
  auto before
    = get_or(args.invocation.options, "explore.before", vast::duration{0s});
  auto after
    = get_or(args.invocation.options, "explore.after", vast::duration{0s});
  auto expl = self->spawn(explorer, self, before, after);
  return expl;
}

} // namespace vast::system
