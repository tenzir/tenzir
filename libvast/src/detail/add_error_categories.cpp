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

#include "vast/error.hpp"

#include "vast/detail/add_error_categories.hpp"

using namespace caf;

namespace vast::detail {

void add_error_categories(caf::actor_system_config& cfg) {
  // Register VAST's custom error type.
  auto vast_renderer = [](uint8_t x, atom_value, const message& msg) {
    std::string result;
    result += "got ";
    switch (static_cast<ec>(x)) {
      default:
        result += to_string(static_cast<ec>(x));
        break;
      case ec::unspecified:
        result += "unspecified error";
        break;
    };
    if (!msg.empty()) {
      result += ": ";
      result += deep_to_string(msg);
    }
    return result;
  };
  auto caf_renderer = [](uint8_t x, atom_value, const message& msg) {
    std::string result;
    result += "got caf::";
    result += to_string(static_cast<sec>(x));
    if (!msg.empty()) {
      result += ": ";
      result += deep_to_string(msg);
    }
    return result;
  };
  cfg.add_error_category(atom("vast"), vast_renderer);
  cfg.add_error_category(atom("system"), caf_renderer);
}

} // namespace vast::detail
