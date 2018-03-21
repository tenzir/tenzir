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
#include "vast/system/export_command.hpp"
#include "vast/system/import_command.hpp"
#include "vast/system/reader_command.hpp"
#include "vast/system/remote_command.hpp"
#include "vast/system/start_command.hpp"
#include "vast/system/writer_command.hpp"

#include "vast/format/ascii.hpp"
#include "vast/format/bgpdump.hpp"
#include "vast/format/bro.hpp"
#include "vast/format/csv.hpp"
#include "vast/format/json.hpp"
#include "vast/format/mrt.hpp"

#ifdef VAST_HAVE_PCAP
#include "vast/system/pcap_reader_command.hpp"
#include "vast/system/pcap_writer_command.hpp"
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
  app.add_command<start_command>("start");
  // Add program composed commands.
  auto import_cmd = app.add_command<import_command>("import");
  import_cmd->add<reader_command<format::bro::reader>>("bro");
  import_cmd->add<reader_command<format::mrt::reader>>("mrt");
  import_cmd->add<reader_command<format::bgpdump::reader>>("bgpdump");
  auto export_cmd = app.add_command<export_command>("export");
  export_cmd->add<writer_command<format::bro::writer>>("bro");
  export_cmd->add<writer_command<format::csv::writer>>("csv");
  export_cmd->add<writer_command<format::ascii::writer>>("ascii");
  export_cmd->add<writer_command<format::json::writer>>("json");
#ifdef VAST_HAVE_PCAP
  import_cmd->add<pcap_reader_command>("pcap");
  export_cmd->add<pcap_writer_command>("pcap");
#endif
  // Add program commands that always run remotely.
  app.add_command<remote_command>("stop");
  app.add_command<remote_command>("show");
  app.add_command<remote_command>("spawn");
  app.add_command<remote_command>("send");
  app.add_command<remote_command>("kill");
  app.add_command<remote_command>("peer");
  // Dispatch to root command.
  auto result = app.run(sys,
                        caf::message_builder{cfg.command_line.begin(),
                                             cfg.command_line.end()}
                        .move_to_message());
  VAST_INFO("shutting down");
  return result;
}
