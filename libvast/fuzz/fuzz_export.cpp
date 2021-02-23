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

#include "vast/event_types.hpp"
#include "vast/logger.hpp"
#include "vast/schema.hpp"
#include "vast/system/application.hpp"
#include "vast/system/default_configuration.hpp"

#include <caf/actor_system.hpp>

const char* arbitrary_data = "some random bytes";

int main(int argc, char* argv[]) {
  if (argc < 1)
    return EXIT_FAILURE;
  vast::system::default_configuration cfg;
  // Application setup.
  auto [root, root_factory] = vast::system::make_application(argv[0]);
  if (!root)
    return EXIT_FAILURE;
  // Parse the CLI.
  const char* query = argc > 1 ? argv[1] : arbitrary_data;
  std::vector<std::string> command_line{"--node", "export", "json", query};
  auto invocation = parse(*root, command_line.begin(), command_line.end());
  if (!invocation) {
    if (invocation.error()) {
      vast::system::render_error(*root, invocation.error(), std::cerr);
      return EXIT_FAILURE;
    }
    // Printing help/documentation texts returns caf::no_error, and we want to
    // indicate success when printing the help/documentation texts.
    return EXIT_SUCCESS;
  }
  // Create log context.
  auto log_context = vast::create_log_context(*invocation, cfg.content);
  if (!log_context)
    return EXIT_FAILURE;
  // Set up the event types singleton.
  if (auto schema = vast::load_schema(cfg)) {
    vast::event_types::init(*std::move(schema));
  } else {
    VAST_ERROR("failed to read schema dirs: {}", schema.error());
    return EXIT_FAILURE;
  }
  // Set up actor system.
  auto sys = caf::actor_system{cfg};
  auto run_error = caf::error{};
  if (auto result = run(*invocation, sys, root_factory); !result)
    run_error = std::move(result.error());
  else
    result->apply({[&](caf::error& err) { run_error = std::move(err); }});
  if (run_error) {
    vast::system::render_error(*root, run_error, std::cerr);
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}