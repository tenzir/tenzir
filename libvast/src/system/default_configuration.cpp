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

#include "vast/system/default_configuration.hpp"

#include <caf/defaults.hpp>
#include <caf/timestamp.hpp>

#include "vast/config.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/system.hpp"
#include "vast/error.hpp"
#include "vast/filesystem.hpp"
#include "vast/system/application.hpp"

namespace vast::system {

path make_log_dirname() {
  auto dir_name = caf::deep_to_string(caf::make_timestamp());
  dir_name += '#';
  dir_name += std::to_string(detail::process_id());
  return path{"log"} / dir_name;
}

default_configuration::default_configuration(std::string application_name)
  : application_name{std::move(application_name)} {
  // Tweak default logging options.
  set("logger.component-blacklist",
      caf::make_config_value_list(caf::atom("caf"), caf::atom("caf_flow"),
                                  caf::atom("caf_stream")));
  set("logger.console-verbosity", caf::atom("INFO"));
  set("logger.console", caf::atom("COLORED"));
  set("logger.file-verbosity", caf::atom("DEBUG"));
  // Allow VAST clusters to form a mesh.
  set("middleman.enable-automatic-connections", true);
  put(content, "system.schema-paths",
      caf::make_config_value_list(
        (path{VAST_INSTALL_PREFIX} / "share" / "vast" / "schema").str()));
}

caf::error default_configuration::setup_log_file() {
  // Adjust logger file name unless the user overrides the default..
  auto default_fn = caf::defaults::logger::file_name;
  if (caf::get_or(*this, "logger.file-name", default_fn) != default_fn)
    return caf::none;
  // Get proper directory path.
  path base_dir = get_or(*this, "system.directory",
                         defaults::system::directory);
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
  if (auto err = create_symlink(log_dir.trim(-1), link_dir))
    return err;
  // Store full path to the log file in config.
  auto log_file = log_dir / application_name + ".log";
  set("logger.file-name", log_file.str());
  return caf::none;
}

void default_configuration::merge_settings(const caf::settings& from,
                                           caf::settings& to) {
  for (auto& [key, value] : from)
    if (caf::holds_alternative<caf::settings>(value)) {
      merge_settings(caf::get<caf::settings>(value), to[key].as_dictionary());
    } else {
      to.insert_or_assign(key, value);
    }
}

// Parses the options from the root command and adds them to the global
// configuration.
caf::error default_configuration::merge_root_options(system::application& app) {
  // Delegate to the root command for argument parsing.
  caf::settings options;
  app.root.options.parse(options, command_line.begin(), command_line.end());
  // Move everything into the system-wide options.
  merge_settings(options, content);
  return caf::none;
}

} // namespace vast::system
