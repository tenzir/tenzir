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

#include "vast/system/spawn_pivoter.hpp"

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/logger.hpp"
#include "vast/system/pivoter.hpp"
#include "vast/system/spawn_arguments.hpp"

namespace vast::system {

maybe_actor spawn_pivoter(node_actor* self, spawn_arguments& args) {
  VAST_DEBUG_ANON(VAST_ARG(args));
  auto& arguments = args.inv.arguments;
  if (arguments.size() < 2)
    return unexpected_arguments(args);
  auto target_name = arguments[0];
  // Parse given expression.
  auto query_begin = std::next(arguments.begin());
  auto expr = system::normalized_and_validated(query_begin, arguments.end());
  if (!expr)
    return expr.error();
  auto handle = self->spawn(pivoter, self, target_name, *expr);
  VAST_VERBOSE(self, "spawned a pivoter for", to_string(*expr));
  return handle;
}

} // namespace vast::system
