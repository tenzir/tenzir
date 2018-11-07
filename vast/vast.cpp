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

#include <caf/actor_system.hpp>
#include <caf/io/middleman.hpp>
#include <caf/message_builder.hpp>

#include "vast/config.hpp"

#ifdef VAST_USE_OPENSSL
#include <caf/openssl/manager.hpp>
#endif

#include "vast/system/default_application.hpp"

using namespace vast;
using namespace vast::system;

namespace {

struct config : configuration {
  config() {
    // Consider only VAST's log messages by default.
    set("logger.component-filter", "vast");
    load<caf::io::middleman>();
    set("middleman.enable-automatic-connections", true);
#ifdef VAST_USE_OPENSSL
    load<caf::openssl::manager>();
#endif
  }
};

} // namespace <anonymous>

int main(int argc, char** argv) {
  // Scaffold
  config cfg;
  cfg.parse(argc, argv);
  cfg.set("logger.console", caf::atom("COLORED"));
  caf::actor_system sys{cfg};
  default_application app;
  app.name("vast");
  // Dispatch to root command.
  auto result = app.run(sys, cfg.command_line.begin(), cfg.command_line.end());
  if (result.match_elements<caf::error>()) {
    std::cerr << sys.render(result.get_as<caf::error>(0)) << std::endl;
    return EXIT_FAILURE;
  }
}
