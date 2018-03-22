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

#include <caf/actor_system.hpp>
#include <caf/message_builder.hpp>

#include "vast/system/default_application.hpp"

using namespace vast;
using namespace vast::system;

int main(int argc, char** argv) {
  // Scaffold
  configuration cfg{argc, argv};
  cfg.logger_console = caf::atom("COLORED");
  caf::actor_system sys{cfg};
  default_application app;
  // Dispatch to root command.
  auto result = app.run(sys,
                        caf::message_builder{cfg.command_line.begin(),
                                             cfg.command_line.end()}
                        .move_to_message());
  return result;
}
