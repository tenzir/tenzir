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

#include "vast/logger.hpp"

#include "vast/system/application.hpp"
#include "vast/system/configuration.hpp"
#include "vast/system/run_export.hpp"
#include "vast/system/run_import.hpp"
#include "vast/system/run_reader.hpp"
#include "vast/system/run_remote.hpp"
#include "vast/system/run_start.hpp"

#include "vast/format/bgpdump.hpp"
#include "vast/format/bro.hpp"
#include "vast/format/mrt.hpp"

#ifdef VAST_HAVE_PCAP
#include "vast/system/run_pcap_reader.hpp"
#endif

using namespace vast;
using namespace vast::system;

int main(int argc, char** argv) {
  // Scaffold
  configuration cfg{argc, argv};
  cfg.logger_console = caf::atom("COLORED");
  caf::actor_system sys{cfg};
  application app;
  // Add program commands that run locally.
  app.add_command<run_start>("start");
  // Add program composed commands.
  auto cmd = app.add_command<run_import>("import");
  cmd->add<run_reader<format::bro::reader>>("bro");
  cmd->add<run_reader<format::mrt::reader>>("mrt");
#ifdef VAST_HAVE_PCAP
  cmd->add<run_pcap_reader>("pcap");
#endif
  cmd->add<run_reader<format::bgpdump::reader>>("bgpdump");
  app.add_command<run_export>("export");
  // Add program commands that always run remotely.
  app.add_command<run_remote>("stop");
  app.add_command<run_remote>("show");
  app.add_command<run_remote>("spawn");
  app.add_command<run_remote>("send");
  app.add_command<run_remote>("kill");
  app.add_command<run_remote>("peer");
  // Dispatch to root command.
  auto result = app.run(sys,
                        caf::message_builder{cfg.command_line.begin(),
                                             cfg.command_line.end()}
                        .move_to_message());
  VAST_INFO("shutting down");
  return result;
}
