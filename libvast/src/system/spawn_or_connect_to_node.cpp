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

#include "vast/system/spawn_or_connect_to_node.hpp"

#include "vast/logger.hpp"
#include "vast/system/connect_to_node.hpp"
#include "vast/system/spawn_node.hpp"

namespace vast::system {

namespace {

using result_t = caf::variant<caf::error, caf::actor, scope_linked_actor>;

} // namespace <anonymous>

result_t spawn_or_connect_to_node(caf::scoped_actor& self,
                                  const caf::config_value_map& opts) {
  VAST_TRACE(VAST_ARG(opts));
  auto convert = [](auto&& result) -> result_t {
    if (result)
      return std::move(*result);
    else
      return std::move(result.error());
  };
  if (caf::get_or<bool>(opts, "node", false))
    return convert(spawn_node(self, opts));
  return convert(connect_to_node(self, opts));
}

} // namespace vast::system
