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

#include "vast/detail/add_error_categories.hpp"

#include "vast/error.hpp" // for to_string, ec

#include <caf/actor_system_config.hpp>     // for actor_system_config
#include <caf/atom.hpp>                    // for atom, atom_value
#include <caf/deep_to_string.hpp>          // for deep_to_string
#include <caf/message.hpp>                 // for message
#include <caf/meta/omittable_if_empty.hpp> // for omittable_if_empty
#include <caf/meta/type_name.hpp>          // for type_name

#include <cstdint> // for uint8_t

using namespace caf;

namespace vast::detail {

void add_error_categories(caf::actor_system_config& cfg) {
  auto vast_renderer = [](uint8_t x, atom_value, const message& msg) {
    return caf::deep_to_string(caf::meta::type_name("vast.ec"),
                               static_cast<ec>(x),
                               caf::meta::omittable_if_empty(), msg);
  };
  cfg.add_error_category(atom("vast"), vast_renderer);
}

} // namespace vast::detail
