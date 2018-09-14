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

#include <algorithm>
#include <iostream>

#include "vast/config.hpp"

#include <caf/message_builder.hpp>
#include <caf/io/middleman.hpp>
#ifdef VAST_USE_OPENCL
#include <caf/opencl/manager.hpp>
#endif
#ifdef VAST_USE_OPENSSL
#include <caf/openssl/manager.hpp>
#endif

#include "vast/config.hpp"

#include "vast/system/configuration.hpp"

#include "vast/detail/add_message_types.hpp"
#include "vast/detail/add_error_categories.hpp"
#include "vast/detail/adjust_resource_consumption.hpp"
#include "vast/detail/string.hpp"
#include "vast/detail/system.hpp"

using namespace caf;

namespace vast::system {

configuration::configuration() {
  detail::add_message_types(*this);
  detail::add_error_categories(*this);
  // Use 'vast.ini' instead of generic 'caf-application.ini'.
  config_file_path = "vast.ini";
  // Load I/O module.
  load<io::middleman>();
  // GPU acceleration.
#ifdef VAST_USE_OPENCL
  load<opencl::manager>();
#endif
}

configuration& configuration::parse(int argc, char** argv) {
  VAST_ASSERT(argc > 0);
  VAST_ASSERT(argv != nullptr);
  command_line.assign(argv + 1, argv + argc);
  // Move CAF options to the end of the command line, parse them, and then
  // remove them.
  auto is_vast_opt = [](auto& x) { return !starts_with(x, "--caf#"); };
  auto caf_opt = std::stable_partition(command_line.begin(),
                                       command_line.end(), is_vast_opt);
  std::vector<std::string> caf_args;
  std::move(caf_opt, command_line.end(), std::back_inserter(caf_args));
  command_line.erase(caf_opt, command_line.end());
  actor_system_config::parse(std::move(caf_args));
  return *this;
}

} // namespace vast::system
