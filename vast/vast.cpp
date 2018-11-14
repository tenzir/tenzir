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
#include <caf/io/middleman.hpp>
#include <caf/message_builder.hpp>

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
  using namespace std::chrono;
  auto secs = duration_cast<seconds>(system_clock::now().time_since_epoch());
  auto pid = detail::process_id();
  // TODO: Use YYYY-MM-DD-HH-MM-SS instead of a UNIX timestamp.
  path dir = std::to_string(secs.count()) + "#" + std::to_string(pid);
  return "log" / dir;
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
  // Setup path for CAF logger.
  // FIXME: use config value instead of this hardcoded hack for 'dir'. This
  // would need parsing the INI file and/or command line at this point, but
  // happens later. Not sure yet what the best solution is. --MV
  auto dir = path::current() / defaults::command::directory;
  auto log_file = setup_log_file(dir.complete());
  if (!log_file) {
    std::cerr << "failed to setup log file: " << to_string(log_file.error())
              << std::endl;
    return EXIT_FAILURE;
  }
  cfg.set("logger.file-name", log_file->str());
  cfg.set("logger.file-verbosity", caf::atom("DEBUG"));
  cfg.set("logger.console", caf::atom("COLORED"));
  // Initialize actor system (and thereby CAF's logger).
  caf::actor_system sys{cfg};
  default_application app;
  app.root.description = "manage a VAST topology";
  app.root.name = argv[0];
  // Skip any path in the application name.
  auto find_slash = [&] { return app.root.name.find('/'); };
  for (auto p = find_slash(); p != std::string_view::npos; p = find_slash())
    app.root.name.remove_prefix(p + 1);
  // Dispatch to root command.
  auto result = app.run(sys, cfg.command_line.begin(), cfg.command_line.end());
  if (result.match_elements<caf::error>()) {
    std::cerr << sys.render(result.get_as<caf::error>(0)) << std::endl;
    return EXIT_FAILURE;
  }
}
