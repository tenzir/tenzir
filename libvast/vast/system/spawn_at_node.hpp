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

#include <caf/actor.hpp>
#include <caf/expected.hpp>
#include <caf/scoped_actor.hpp>

namespace vast::system {

template <typename... Arguments>
caf::expected<caf::actor>
spawn_at_node(caf::scoped_actor& self, caf::actor node, Arguments&&... xs) {
  caf::expected<caf::actor> result = caf::no_error;
  self->request(node, caf::infinite, std::forward<Arguments>(xs)...)
    .receive([&](caf::actor& a) { result = std::move(a); },
             [&](caf::error& e) { result = std::move(e); });
  return result;
}

} // namespace vast::system
