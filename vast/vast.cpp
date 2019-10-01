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
#include <caf/atom.hpp>
#include <caf/io/middleman.hpp>
#include <caf/timestamp.hpp>

#include "vast/config.hpp"

#ifdef VAST_USE_OPENSSL
#include <caf/openssl/manager.hpp>
#endif

#include "vast/error.hpp"
#include "vast/event_types.hpp"
#include "vast/filesystem.hpp"
#include "vast/logger.hpp"
#include "vast/schema.hpp"

#include "vast/system/default_application.hpp"
#include "vast/system/default_configuration.hpp"

#include "vast/detail/process.hpp"
#include "vast/detail/system.hpp"

using namespace vast;
using namespace vast::system;

int main(int argc, char** argv) {
  // CAF scaffold.
  default_configuration cfg;
  if (auto err = cfg.parse(argc, argv)) {
    std::cerr << "failed to parse configuration: " << to_string(err)
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
  // Parse CLI.
  auto maybe_invocation = parse(app.root, cfg.command_line.begin(),
                          cfg.command_line.end());
  if (!maybe_invocation) {
    render_error(app, maybe_invocation.error(), std::cerr);
    return EXIT_FAILURE;
  }
  if (get_or(maybe_invocation->options, "help", false)) {
    helptext(*maybe_invocation->target, std::cerr);
    return EXIT_SUCCESS;
  }
  if (get_or(maybe_invocation->options, "documentation", false)) {
    documentationtext(*maybe_invocation->target, std::cerr);
    return EXIT_SUCCESS;
  }
  // Initialize actor system (and thereby CAF's logger).
  if (!init_config(cfg, *maybe_invocation, std::cerr))
    return EXIT_FAILURE;
  caf::actor_system sys{cfg};
  // Get filesystem path to the executable.
  auto binary = detail::objectpath();
  if (!binary) {
    VAST_ERROR_ANON("failed to get program path");
    return EXIT_FAILURE;
  }
  auto vast_share = binary->parent().parent() / "share" / "vast";
  // Load event types.
  auto default_dirs = std::vector{vast_share / "schema"};
  using string_list = std::vector<std::string>;
  if (auto user_dirs = caf::get_if<string_list>(&cfg, "system.schema-paths"))
    default_dirs.insert(default_dirs.end(), user_dirs->begin(),
                        user_dirs->end());
  if (auto schema = load_schema(default_dirs)) {
    event_types::init(*std::move(schema));
  } else {
    VAST_ERROR_ANON("failed to read schema dirs:", to_string(schema.error()));
    return EXIT_FAILURE;
  }
  // Dispatch to root command.
  auto maybe_result = run(*maybe_invocation, sys);
  if (!maybe_result) {
    render_error(app, maybe_result.error(), std::cerr);
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
