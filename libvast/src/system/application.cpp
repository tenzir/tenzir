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

#include "vast/system/application.hpp"

#include <iostream>

#include <caf/atom.hpp>
#include <caf/error.hpp>

#include "vast/detail/adjust_resource_consumption.hpp"
#include "vast/detail/assert.hpp"
#include "vast/error.hpp"

using std::string;

using namespace std::chrono_literals;
using namespace std::string_literals;
using namespace caf;

namespace vast::system {

application::application() {
  // TODO: this function has side effects...should we put it elsewhere where
  // it's explicit to the user? Or perhaps make whatever this function does
  // simply a configuration option and use it later?
  detail::adjust_resource_consumption();
}

void render_error(const application& app, const caf::error& err,
                  std::ostream& os) {
  if (!err)
    // The user most likely killed the process via CTRL+C, print nothing.
    return;
  os << render(err);
  if (err.category() == caf::atom("vast")) {
    auto x = static_cast<vast::ec>(err.code());
    switch (x) {
      default:
        break;
      case ec::invalid_subcommand:
      case ec::missing_subcommand:
      case ec::unrecognized_option: {
        auto ctx = err.context();
        if (ctx.match_element<std::string>(1)) {
          auto name = ctx.get_as<std::string>(1);
          if (auto cmd = resolve(app.root, name))
            helptext(*cmd, os);
        }
        else {
          VAST_ASSERT("User visible error contexts must consist of strings!");
        }
        break;
      }
    }
  }
}

} // namespace vast::system
