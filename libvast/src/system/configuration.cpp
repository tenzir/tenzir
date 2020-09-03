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

#include "vast/system/configuration.hpp"

#include "vast/config.hpp"
#include "vast/detail/add_message_types.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/process.hpp"
#include "vast/detail/string.hpp"
#include "vast/detail/system.hpp"
#include "vast/path.hpp"
#include "vast/synopsis_factory.hpp"
#include "vast/table_slice_builder_factory.hpp"
#include "vast/table_slice_factory.hpp"
#include "vast/value_index.hpp"
#include "vast/value_index_factory.hpp"

#include <caf/io/middleman.hpp>
#include <caf/message_builder.hpp>
#if VAST_USE_OPENCL
#  include <caf/opencl/manager.hpp>
#endif
#if VAST_USE_OPENSSL
#  include <caf/openssl/manager.hpp>
#endif

#include <algorithm>
#include <iostream>

namespace vast::system {

namespace {

template <class... Ts>
void initialize_factories() {
  (factory<Ts>::initialize(), ...);
}

} // namespace

configuration::configuration() {
  detail::add_message_types(*this);
  // We must clear the config_file_path first so it does not use
  // `caf-application.ini` as fallback.
  config_file_path.clear();
  // Instead of the CAF-supplied `config_file_path`, we use our own
  // `config_paths` variable in order to support multiple configuration files.
  if (const char* xdg_config_home = std::getenv("XDG_CONFIG_HOME"))
    config_paths.emplace_back(path{xdg_config_home} / "vast" / "vast.conf");
  else if (const char* home = std::getenv("HOME"))
    config_paths.emplace_back(path{home} / ".config" / "vast" / "vast.conf");
  config_paths.emplace_back(VAST_SYSCONFDIR "/vast/vast.conf");
  // Remove all non-existent config files.
  config_paths.erase(
    std::remove_if(config_paths.begin(), config_paths.end(),
                   [](auto&& p) { return !p.is_regular_file(); }),
    config_paths.end());
  // Load I/O module.
  load<caf::io::middleman>();
  // GPU acceleration.
#if VAST_USE_OPENCL
  load<caf::opencl::manager>();
#endif
  initialize_factories<synopsis, table_slice, table_slice_builder,
                       value_index>();
}

caf::error configuration::parse(int argc, char** argv) {
  VAST_ASSERT(argc > 0);
  VAST_ASSERT(argv != nullptr);
  command_line.assign(argv + 1, argv + argc);
  // Move CAF options to the end of the command line, parse them, and then
  // remove them.
  auto is_vast_opt = [](auto& x) {
    return !(detail::starts_with(x, "--caf.")
             || detail::starts_with(x, "--config=")
             || detail::starts_with(x, "--config-file="));
  };
  auto caf_opt = std::stable_partition(command_line.begin(), command_line.end(),
                                       is_vast_opt);
  std::vector<std::string> caf_args;
  std::move(caf_opt, command_line.end(), std::back_inserter(caf_args));
  command_line.erase(caf_opt, command_line.end());
  for (auto& arg : caf_args) {
    // Remove caf. prefix for CAF parser.
    if (detail::starts_with(arg, "--caf."))
      arg.erase(2, 4);
    // Rewrite --config= option to CAF's expexted format.
    if (detail::starts_with(arg, "--config="))
      arg.replace(8, 0, "-file");
  }
  for (const auto& p : config_paths) {
    if (auto err = actor_system_config::parse(caf_args, p.str().c_str())) {
      err.context() += caf::make_message(p);
      return err;
    }
  }
  return actor_system_config::parse(caf_args);
}

} // namespace vast::system
