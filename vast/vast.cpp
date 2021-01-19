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

#include "vast/config.hpp"
#include "vast/detail/process.hpp"
#include "vast/detail/stable_set.hpp"
#include "vast/detail/system.hpp"
#include "vast/detail/settings.hpp"
#include "vast/error.hpp"
#include "vast/event_types.hpp"
#include "vast/logger.hpp"
#include "vast/path.hpp"
#include "vast/schema.hpp"
#include "vast/system/application.hpp"
#include "vast/system/default_configuration.hpp"

#include <caf/actor_system.hpp>
#include <caf/atom.hpp>
#include <caf/io/middleman.hpp>
#include <caf/settings.hpp>
#include <caf/timestamp.hpp>

#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#if VAST_USE_OPENSSL
#  include <caf/openssl/manager.hpp>
#endif

using namespace vast;
using namespace vast::system;

int main(int argc, char** argv) {
  // Set up our configuration, e.g., load of YAML config file(s).
  default_configuration cfg;
  if (auto err = cfg.parse(argc, argv)) {
    std::cerr << "failed to parse configuration: " << to_string(err)
              << std::endl;
    return EXIT_FAILURE;
  }
  // Application setup.
  const auto [root, factory] = make_application(argv[0]);
  if (!root)
    return EXIT_FAILURE;
  // Parse CLI.
  auto invocation
    = parse(*root, cfg.command_line.begin(), cfg.command_line.end());
  if (!invocation) {
    if (invocation.error()) {
      render_error(*root, invocation.error(), std::cerr);
      return EXIT_FAILURE;
    }
    // Printing help/documentation returns a no_error, and we want to indicate
    // success when printing the help/documentation texts.
    return EXIT_SUCCESS;
  }

  auto log_context = vast::create_log_context(*invocation, cfg.content) ;
  if (!log_context)
    return EXIT_FAILURE ;

  vast::detail::merge_settings((*invocation).options, cfg.content);
  caf::actor_system sys{cfg};

  if (!cfg.config_file_path.empty())
    cfg.config_files.emplace_back(std::move(cfg.config_file_path));
  for (auto& path : cfg.config_files)
    VAST_INFO_ANON("loaded configuration file:", path);
  // Load event types.
  if (auto schema = load_schema(cfg)) {
    event_types::init(*std::move(schema));
  } else {
    VAST_ERROR_ANON("failed to read schema dirs:", render(schema.error()));
    return EXIT_FAILURE;
  }
  // Dispatch to root command.
  auto result = run(*invocation, sys, factory);
  if (!result) {
    render_error(*root, result.error(), std::cerr);
    return EXIT_FAILURE;
  }
  if (result->match_elements<caf::error>()) {
    auto& err = result->get_as<caf::error>(0);
    if (err) {
      vast::system::render_error(*root, err, std::cerr);
      return EXIT_FAILURE;
    }
  }
  return EXIT_SUCCESS;
}
