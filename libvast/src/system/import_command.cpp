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

#include "vast/system/import_command.hpp"

#include <iostream>

#include <caf/all.hpp>
#include <caf/io/all.hpp>
#ifdef VAST_USE_OPENSSL
#include <caf/openssl/all.hpp>
#endif // VAST_USE_OPENSSL

#include "vast/logger.hpp"

#include "vast/system/signal_monitor.hpp"
#include "vast/system/spawn.hpp"

using namespace caf;

namespace vast::system {
using namespace std::chrono_literals;

import_command::import_command(command* parent, std::string_view name)
  : node_command{parent, name} {
  // nop
}

int import_command::run_impl(actor_system&, XXoption_mapXX&, argument_iterator,
                             argument_iterator) {
  VAST_ERROR("import_command::run_impl called");
  return EXIT_FAILURE;
}

} // namespace vast::system
