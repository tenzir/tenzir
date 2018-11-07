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

#include "vast/system/default_application.hpp"

#include "vast/system/application.hpp"
#include "vast/system/configuration.hpp"
#include "vast/system/export_command.hpp"
#include "vast/system/generator_command.hpp"
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
#include "vast/format/test.hpp"

#ifdef VAST_HAVE_PCAP
#include "vast/system/pcap_reader_command.hpp"
#include "vast/system/pcap_writer_command.hpp"
#endif

#define ADD_READER(ReaderFormat, Description)                                  \
  import_->add<reader_command<format::ReaderFormat ::reader>>(#ReaderFormat,   \
                                                              Description)

#define ADD_GENERATOR(ReaderFormat, Description)                               \
  import_->add<generator_command<format::ReaderFormat ::reader>>(              \
    #ReaderFormat, Description)

#define ADD_WRITER(ReaderFormat, Description)                                  \
  export_->add<writer_command<format::ReaderFormat ::writer>>(#ReaderFormat,   \
                                                              Description)

namespace vast::system {

default_application::default_application() {
  // Add program commands that run locally.
  add<start_command>("start", "starts a node");
  // Add program composed commands.
  import_ = add<import_command>("import", "imports data from STDIN or file");
  ADD_READER(bro, "imports Bro logs from STDIN or file");
  ADD_READER(mrt, "imports MRT logs from STDIN or file");
  ADD_READER(bgpdump, "imports Bro logs from STDIN or file");
  ADD_GENERATOR(test, "imports random data for testing or benchmarking");
  export_ = add<export_command>("export",
                                "exports query results to STDOUT or file");
  ADD_WRITER(bro, "exports query results in Bro format");
  ADD_WRITER(csv, "exports query results in CSV format");
  ADD_WRITER(ascii, "exports query results in ASCII format");
  ADD_WRITER(json, "exports query results in JSON format");
#ifdef VAST_HAVE_PCAP
  import_->add<pcap_reader_command>("pcap",
                                    "imports PCAP logs from STDIN or file");
  export_->add<pcap_writer_command>("pcap",
                                    "exports query results in PCAP format");
#endif
  // Add program commands that always run remotely.
  add<remote_command>("stop", "stops a node");
  add<remote_command>("show", "shows various properties of a topology");
  add<remote_command>("spawn", "creates a new component");
  add<remote_command>("kill", "terminates a component");
  add<remote_command>("peer", "peers with another node");
}

} // namespace vast::system
