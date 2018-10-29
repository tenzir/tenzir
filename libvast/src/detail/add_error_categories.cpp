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

#include <cstdint>
#include <string>

#include <caf/actor_system_config.hpp>
#include <caf/deep_to_string.hpp>
#include <caf/meta/omittable_if_empty.hpp>
#include <caf/meta/type_name.hpp>

#include "vast/error.hpp"

#include "vast/detail/add_error_categories.hpp"

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
