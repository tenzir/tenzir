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

#include <algorithm>
#include <iostream>

#include "vast/config.hpp"

#include <caf/message_builder.hpp>
#include <caf/io/middleman.hpp>
#if VAST_USE_OPENCL
#  include <caf/opencl/manager.hpp>
#endif
#if VAST_USE_OPENSSL
#  include <caf/openssl/manager.hpp>
#endif

#include "vast/detail/add_message_types.hpp"
#include "vast/detail/adjust_resource_consumption.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/string.hpp"
#include "vast/detail/system.hpp"
#include "vast/filesystem.hpp"
#include "vast/synopsis_factory.hpp"
#include "vast/table_slice_builder_factory.hpp"
#include "vast/table_slice_factory.hpp"
#include "vast/value_index.hpp"
#include "vast/value_index_factory.hpp"

#if VAST_HAVE_ARROW
#  include "vast/arrow_table_slice.hpp"
#  include "vast/arrow_table_slice_builder.hpp"
#endif

using namespace caf;

namespace vast::system {

namespace {

template <class... Ts>
void initialize_factories() {
  (factory<Ts>::initialize(), ...);
}

} // namespace <anonymous>

configuration::configuration() {
  detail::add_message_types(*this);
  // Use 'vast.conf' instead of generic 'caf-application.ini' and fall back to
  // $PREFIX/etc/vast if no local vast.conf exists.
  if ((path::current() / "vast.conf").is_regular_file()) {
    config_file_path = "vast.conf";
  } else {
    auto global_conf = path{VAST_INSTALL_PREFIX} / "etc" / "vast" / "vast.conf";
    if (!global_conf.is_regular_file())
      global_conf = path{"/etc/vast/vast.conf"};
    config_file_path = global_conf.str();
  }
  // Load I/O module.
  load<io::middleman>();
  // GPU acceleration.
#if VAST_USE_OPENCL
  load<opencl::manager>();
#endif
  initialize_factories<synopsis, table_slice, table_slice_builder,
                       value_index>();
#if VAST_HAVE_ARROW
  factory<vast::table_slice>::add<arrow_table_slice>();
  factory<vast::table_slice_builder>::add<arrow_table_slice_builder>(
    arrow_table_slice::class_id);
#endif
}

caf::error configuration::parse(int argc, char** argv) {
  VAST_ASSERT(argc > 0);
  VAST_ASSERT(argv != nullptr);
  command_line.assign(argv + 1, argv + argc);
  // Move CAF options to the end of the command line, parse them, and then
  // remove them.
  auto is_vast_opt = [](auto& x) {
    return !(starts_with(x, "--caf.") || starts_with(x, "--config=")
             || starts_with(x, "--config-file="));
  };
  auto caf_opt = std::stable_partition(command_line.begin(),
                                       command_line.end(), is_vast_opt);
  std::vector<std::string> caf_args;
  std::move(caf_opt, command_line.end(), std::back_inserter(caf_args));
  command_line.erase(caf_opt, command_line.end());
  for (auto& arg : caf_args) {
    // Remove caf. prefix for CAF parser.
    if (starts_with(arg, "--caf."))
      arg.erase(2, 4);
    // Rewrite --config= option to CAF's expexted format.
    if (starts_with(arg, "--config="))
      arg.replace(8, 0, "-file");
  }
  return actor_system_config::parse(std::move(caf_args));
}

} // namespace vast::system
