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

#include "vast/logger.hpp"

#include "vast/system/application.hpp"
#include "vast/system/configuration.hpp"
#include "vast/system/run_export.hpp"
#include "vast/system/run_import.hpp"
#include "vast/system/run_start.hpp"
#include "vast/system/run_remote.hpp"

using namespace vast;

int main(int argc, char** argv) {
  VAST_TRACE("");
  // Scaffold
  system::configuration cfg{argc, argv};
  caf::actor_system sys{cfg};
  system::application app;
  // Add program commands that run locally.
  app.add_command<system::run_start>("start");
  // Add program composed commands.
  app.add_command<system::run_import>("import");
  app.add_command<system::run_export>("export");
  // Add program commands that always run remotely.
  app.add_command<system::run_remote>("stop");
  app.add_command<system::run_remote>("show");
  app.add_command<system::run_remote>("spawn");
  app.add_command<system::run_remote>("send");
  app.add_command<system::run_remote>("kill");
  app.add_command<system::run_remote>("peer");
  // Dispatch to root command.
  return app.run(sys,
                 caf::message_builder{cfg.command_line.begin(),
                                      cfg.command_line.end()}
                 .move_to_message());
}
