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

#include "vast/system/help_command.hpp"

#include <string>
#include <vector>

#include <caf/config_value.hpp>
#include <caf/message.hpp>
#include <caf/settings.hpp>

namespace vast::system {

caf::message help_command(const command& cmd, caf::actor_system& sys,
                          caf::settings& , command::argument_iterator,
                          command::argument_iterator) {
  // Simply dispatch to '<root> -h'.
  caf::settings options;
  std::vector<std::string> cli;
  options.emplace("help", caf::config_value{true});
  return run(root(cmd), sys, options, cli.begin(), cli.end());
}

} // namespace vast::system
