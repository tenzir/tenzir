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

#include <cstdlib>

#include <caf/actor_system.hpp>
#include <caf/io/middleman.hpp>
#include <caf/timestamp.hpp>

#include "vast/config.hpp"

#ifdef VAST_USE_OPENSSL
#include <caf/openssl/manager.hpp>
#endif

#include "vast/application_setup.hpp"
#include "vast/filesystem.hpp"

#include "vast/system/default_application.hpp"

#include "vast/detail/system.hpp"

using namespace vast;
using namespace vast::system;

int main(int argc, char** argv) {
  // CAF scaffold.
  config cfg;
  if (auto err = cfg.parse(argc, argv)) {
    std::cerr << "Failed to parse configuration " << to_string(err)
              << std::endl;
    return EXIT_FAILURE;
  }
  // Application setup.
  default_application app;
  app.root.description = "manage a VAST topology";
  app.root.name = argv[0];
  // We're only interested in the application name, not in its path. For
  // example, argv[0] might contain "./build/release/bin/vast" and we are only
  // interested in "vast".
  auto find_slash = [&] { return app.root.name.find('/'); };
  for (auto p = find_slash(); p != std::string_view::npos; p = find_slash())
    app.root.name.remove_prefix(p + 1);
  cfg.merge_root_options(app);
  // Initialize actor system (and thereby CAF's logger).
  caf::actor_system sys{cfg};
  // Dispatch to root command.
  auto result = app.run(sys, cfg.command_line.begin(), cfg.command_line.end());
  if (result.match_elements<caf::error>()) {
    auto& err = result.get_as<caf::error>(0);
    if (err)
      std::cerr << sys.render(err) << std::endl;
    // else: The user most likely killed the process via CTRL+C, print nothing.
    return EXIT_FAILURE;
  }
}
