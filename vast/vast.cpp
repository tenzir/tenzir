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

#include <chrono>

#include <caf/actor_system.hpp>
#include <caf/defaults.hpp>
#include <caf/io/middleman.hpp>
#include <caf/message_builder.hpp>
#include <caf/timestamp.hpp>

#include "vast/config.hpp"

#ifdef VAST_USE_OPENSSL
#include <caf/openssl/manager.hpp>
#endif

#include "vast/defaults.hpp"
#include "vast/error.hpp"
#include "vast/filesystem.hpp"

#include "vast/system/default_application.hpp"

#include "vast/detail/system.hpp"

using namespace vast;
using namespace vast::system;

namespace {

path make_log_dirname() {
  auto dir_name = caf::deep_to_string(caf::make_timestamp());
  dir_name += '#';
  dir_name += std::to_string(detail::process_id());
  return path{"log"} / dir_name;
}

caf::expected<path> setup_log_file(const path& base_dir) {
  auto log_dir = base_dir / make_log_dirname();
  // Create the log directory first, which we need to create the symlink
  // afterwards.
  if (!exists(log_dir))
    if (auto res = mkdir(log_dir); !res)
      return res.error();
  // Create user-friendly symlink to current log directory.
  auto link_dir = log_dir.chop(-1) / "current";
  if (exists(link_dir))
    if (!rm(link_dir))
      return make_error(ec::filesystem_error, "cannot remove log symlink");
  create_symlink(log_dir.trim(-1), link_dir);
  return log_dir / "vast.log";
}

struct config : configuration {
  config() {
    // Tweak default logging options.
    set("logger.component-filter", "vast");
    set("logger.console", caf::atom("COLORED"));
    set("logger.file-verbosity", caf::atom("DEBUG"));
    // Allow VAST clusters to form a mesh.
    set("middleman.enable-automatic-connections", true);
    // Load CAF modules.
    load<caf::io::middleman>();
#ifdef VAST_USE_OPENSSL
    load<caf::openssl::manager>();
#endif
  }

  // Parses the options from the root command and adds them to the global
  // configuration.
  void merge_root_options(application& app) {
    // Delegate to the root command for argument parsing.
    caf::settings options;
    app.root.options.parse(options, command_line.begin(), command_line.end());
    // Move everything into the system-wide options, but use "vast" as category
    // instead of the default "global" category.
    auto& src = options["global"].as_dictionary();
    src["vast"].as_dictionary().insert(src.begin(), src.end());
  }
};

} // namespace <anonymous>

int main(int argc, char** argv) {
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
  // CAF scaffold.
  config cfg;
  cfg.parse(argc, argv);
  cfg.merge_root_options(app);
  // Setup path for CAF logger if not explicitly specified by the user.
  auto default_fn = caf::defaults::logger::file_name;
  if (caf::get_or(cfg, "logger.file-name", default_fn) == default_fn) {
    path dir = get_or(cfg, "vast.dir", defaults::command::directory);
    if (auto log_file = setup_log_file(dir.complete()); !log_file) {
      std::cerr << "failed to setup log file: " << to_string(log_file.error())
                << std::endl;
      return EXIT_FAILURE;
    } else {
      cfg.set("logger.file-name", log_file->str());
    }
  }
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
