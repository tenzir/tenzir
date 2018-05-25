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

#include "vast/system/export_command.hpp"

#include <iostream>

#include <caf/all.hpp>
#include <caf/io/all.hpp>
#ifdef VAST_USE_OPENSSL
#include <caf/openssl/all.hpp>
#endif // VAST_USE_OPENSSL

#include "vast/logger.hpp"
#include "vast/defaults.hpp"

#include "vast/system/signal_monitor.hpp"
#include "vast/system/spawn.hpp"

using namespace caf;

namespace vast::system {
using namespace std::chrono_literals;

export_command::export_command(command* parent, std::string_view name)
  : node_command{parent, name} {
  using namespace vast::defaults;
  add_opt("continuous,c", "marks a query as continuous",
          export_command_continuous);
  add_opt("historical,h", "marks a query as historical",
          export_command_historical);
  add_opt("unified,u", "marks a query as unified", export_command_unified);
  add_opt("events,e", "maximum number of results", export_command_events);
}

int export_command::run_impl(actor_system&, const option_map&, argument_iterator,
                             argument_iterator) {
  VAST_ERROR("export_command::run_impl called");
  return EXIT_FAILURE;
}

} // namespace vast::system
