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

#include "vast/logger.hpp"

using namespace caf;

namespace vast::system {
using namespace std::chrono_literals;

import_command::import_command(command* parent) : node_command(parent) {
  add_opt<bool>("blocking,b", "block until the IMPORTER forwarded all data");
}

caf::message import_command::run_impl(actor_system&,
                                      const caf::config_value_map&,
                                      argument_iterator, argument_iterator) {
  return wrap_error(ec::syntax_error, "missing subcommand to import");
}

} // namespace vast::system
