//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/atoms.hpp"
#include "vast/concept/convertible/to.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/data.hpp"
#include "vast/config.hpp"
#include "vast/data.hpp"
#include "vast/detail/settings.hpp"
#include "vast/detail/signal_handlers.hpp"
#include "vast/detail/system.hpp"
#include "vast/error.hpp"
#include "vast/event_types.hpp"
#include "vast/factory.hpp"
#include "vast/format/reader_factory.hpp"
#include "vast/format/writer_factory.hpp"
#include "vast/logger.hpp"
#include "vast/module.hpp"
#include "vast/plugin.hpp"
#include "vast/system/application.hpp"
#include "vast/system/default_configuration.hpp"
#include "vast/system/make_transforms.hpp"

#include <caf/actor_system.hpp>

#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

int main(int argc, char** argv) {
  using namespace vast;
  // Set a signal handler for fatal conditions. Prints a backtrace if support
  // for that is enabled.
  std::signal(SIGSEGV, fatal_handler);
  std::signal(SIGABRT, fatal_handler);
  // Set up our configuration, e.g., load of YAML config file(s).
  system::default_configuration cfg;
  if (auto err = cfg.parse(argc, argv)) {
    std::cerr << "failed to parse configuration: " << to_string(err)
              << std::endl;
    return EXIT_FAILURE;
  }
  auto loaded_plugin_paths = plugins::load({VAST_BUNDLED_PLUGINS}, cfg);
  if (!loaded_plugin_paths) {
    fmt::print(stderr, "{}\n", loaded_plugin_paths.error());
    return EXIT_FAILURE;
  }
  // Initialize factories.
  factory<format::reader>::initialize();
  factory<format::writer>::initialize();
  // Application setup.
  auto [root, root_factory] = system::make_application(argv[0]);
  if (!root)
    return EXIT_FAILURE;
  // Parse the CLI.
  auto invocation
    = parse(*root, cfg.command_line.begin(), cfg.command_line.end());
  if (!invocation) {
    if (invocation.error()) {
      system::render_error(*root, invocation.error(), std::cerr);
      return EXIT_FAILURE;
    }
    // Printing help/documentation texts returns caf::no_error, and we want to
    // indicate success when printing the help/documentation texts.
    return EXIT_SUCCESS;
  }
  // Merge the options from the CLI into the options from the configuration.
  // From here on, options from the command line can be used.
  detail::merge_settings(invocation->options, cfg.content,
                         policy::merge_lists::yes);
  // Create log context as soon as we know the correct configuration.
  auto log_context = create_log_context(*invocation, cfg.content);
  if (!log_context)
    return EXIT_FAILURE;
  // Print the configuration file(s) that were loaded.
  if (!cfg.config_file_path.empty())
    cfg.config_files.emplace_back(std::move(cfg.config_file_path));
  for (auto& file : cfg.config_files)
    VAST_INFO("loaded configuration file: {}", file);
  // Print the plugins that were loaded, and errors that occured during loading.
  for (const auto& file : *loaded_plugin_paths)
    VAST_VERBOSE("loaded plugin: {}", file);
  // Initialize successfully loaded plugins.
  if (auto err = plugins::initialize(cfg)) {
    VAST_ERROR("failed to initialize plugins: {}", err);
    return EXIT_FAILURE;
  }
  // Issue deprecation warnings.
  for (std::string_view option : {
         "vast.import.batch-encoding",
         "vast.spawn.source.batch-encoding",
         "vast.metrics.self-sink.slice-type",
       })
    if (caf::get_or(cfg, option, "arrow") == std::string_view{"msgpack"})
      VAST_WARN("The 'msgpack' option for the configuration option '{}' is "
                "deprecated; automatically using the 'arrow' encoding instead",
                option);
  if (auto meta_index_fp_rate = caf::get_if<double>( //
        &cfg, "vast.meta-index-fp-rate")) {
    if (auto catalog_fp_rate = caf::get_if<double>( //
          &cfg, "vast.catalog-fp-rate")) {
      VAST_ERROR("The 'vast.meta-index-fp-rate' option is deprecated; please "
                 "remove it from your configuration");
      return EXIT_FAILURE;
    }
    VAST_WARN("The 'vast.meta-index-fp-rate' option is deprecated; "
              "automatically setting its replacement 'vast.catalog-fp-rate' "
              "instead");
    caf::put(cfg.content, "vast.catalog-fp-rate", *meta_index_fp_rate);
  }
  // Eagerly verify the export transform configuration, to avoid hidden
  // configuration errors that pop up the first time a user tries to run
  // `vast export`.
  if (auto export_transforms = make_transforms(
        system::transforms_location::server_export, cfg.content);
      !export_transforms) {
    VAST_ERROR("invalid export transform configuration: {}",
               export_transforms.error());
    return EXIT_FAILURE;
  }
  // Set up the event types singleton.
  if (auto schema = load_schema(cfg)) {
    event_types::init(*std::move(schema));
  } else {
    VAST_ERROR("failed to read schema dirs: {}", schema.error());
    return EXIT_FAILURE;
  }
  // Lastly, initialize the actor system context, and execute the given command.
  // From this point onwards, do not execute code that is not thread-safe.
  auto sys = caf::actor_system{cfg};
  auto run_error = caf::error{};
  if (auto result = run(*invocation, sys, root_factory); !result)
    run_error = std::move(result.error());
  else
    result->apply({[&](caf::error& err) {
      run_error = std::move(err);
    }});
  if (run_error) {
    system::render_error(*root, run_error, std::cerr);
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
