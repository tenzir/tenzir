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

#include "vast/detail/unbox_var.hpp"
#include "vast/logger.hpp"
#include "vast/system/pivoter.hpp"
#include "vast/system/spawn_arguments.hpp"

namespace vast::system {

maybe_actor spawn_pivoter(node_actor* self, spawn_arguments& args) {
  VAST_DEBUG_ANON(VAST_ARG(args));
  auto& arguments = args.invocation.arguments;
  if (arguments.size() < 2)
    return unexpected_arguments(args);
  auto target_name = arguments[0];
  // Parse given expression.
  auto query_begin = std::next(arguments.begin());
  VAST_UNBOX_VAR(expr, normalized_and_validated(query_begin, arguments.end()));
  return self->spawn(pivoter, self, target_name, std::move(expr));
}

} // namespace vast::system
