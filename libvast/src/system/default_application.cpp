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

#include "vast/detail/assert.hpp"
#include "vast/format/ascii.hpp"
#include "vast/format/bgpdump.hpp"
#include "vast/format/csv.hpp"
#include "vast/format/json.hpp"
#include "vast/format/mrt.hpp"
#include "vast/format/test.hpp"
#include "vast/format/zeek.hpp"
#include "vast/system/application.hpp"
#include "vast/system/configuration.hpp"
#include "vast/system/generator_command.hpp"
#include "vast/system/reader_command.hpp"
#include "vast/system/remote_command.hpp"
#include "vast/system/start_command.hpp"
#include "vast/system/version_command.hpp"
#include "vast/system/writer_command.hpp"

#ifdef VAST_HAVE_PCAP
#include "vast/system/pcap_reader_command.hpp"
#include "vast/system/pcap_writer_command.hpp"
#endif

namespace vast::system {

default_application::default_application() {
  // Set global options.
  root.options
    .add<std::string>("directory,d", "directory for persistent state")
    .add<std::string>("endpoint,e", "node endpoint")
    .add<std::string>("id,i", "the unique ID of this node");
  // Default options for commands.
  auto opts = [] { return command::opts(); };
  // Add standalone commands.
  add(version_command, "version", "prints the software version", opts());
  add(start_command, "start", "starts a node",
      opts()
        .add<std::string>("aging-query,a",
                          "query to periodically erase aged data")
        .add<caf::timespan>("aging-frequency,f",
                            "frequency for triggering the aging query"));
  add(remote_command, "stop", "stops a node", opts());
  add(remote_command, "spawn", "creates a new component", opts());
  add(remote_command, "kill", "terminates a component", opts());
  add(remote_command, "peer", "peers with another node", opts());
  add(remote_command, "status", "shows various properties of a topology",
      opts());
  // Add "import" command and its children.
  import_ = add(nullptr, "import", "imports data from STDIN or file",
                opts()
                  .add<caf::atom_value>("table-slice,t", "table slice type")
                  .add<bool>("node,n",
                             "spawn a node instead of connecting to one")
                  .add<bool>("blocking,b",
                             "block until the IMPORTER forwarded all data"));
  import_->add(reader_command<format::zeek::reader>, "zeek",
               "imports Zeek logs from STDIN or file", src_opts());
  import_->add(reader_command<format::mrt::reader>, "mrt",
               "imports MRT logs from STDIN or file", src_opts());
  import_->add(reader_command<format::bgpdump::reader>, "bgpdump",
               "imports BGPdump logs from STDIN or file", src_opts());
  import_->add(generator_command<format::test::reader>, "test",
               "imports random data for testing or benchmarking",
               opts()
                 .add<size_t>("seed", "the random seed")
                 .add<size_t>("num,N", "events to generate"));
  // Add "export" command and its children.
  export_ = add(nullptr, "export", "exports query results to STDOUT or file",
                opts()
                  .add<std::string>("read,r", "path for reading the query")
                  .add<bool>("node,n",
                             "spawn a node instead of connecting to one")
                  .add<bool>("continuous,c", "marks a query as continuous")
                  .add<bool>("historical,h", "marks a query as historical")
                  .add<bool>("unified,u", "marks a query as unified")
                  .add<size_t>("events,e", "maximum number of results"));
  export_->add(writer_command<format::zeek::writer>, "zeek",
               "exports query results in Zeek format", snk_opts());
  export_->add(writer_command<format::csv::writer>, "csv",
               "exports query results in CSV format", snk_opts());
  export_->add(writer_command<format::ascii::writer>, "ascii",
               "exports query results in ASCII format", snk_opts());
  export_->add(writer_command<format::json::writer>, "json",
               "exports query results in JSON format", snk_opts());
  // Add PCAP import and export commands when compiling with PCAP enabled.
#ifdef VAST_HAVE_PCAP
  import_->add(
    pcap_reader_command, "pcap", "imports PCAP logs from STDIN or file",
    opts()
      .add<std::string>("read,r", "path to input where to read events from")
      .add<std::string>("schema,s", "path to alternate schema")
      .add<bool>("uds,d", "treat -r as listening UNIX domain socket")
      .add<size_t>("cutoff,c", "skip flow packets after this many bytes")
      .add<size_t>("flow-max,m", "number of concurrent flows to track")
      .add<size_t>("flow-age,a", "max flow lifetime before eviction")
      .add<size_t>("flow-expiry,e", "flow table expiration interval")
      .add<size_t>("pseudo-realtime,p", "factor c delaying packets by 1/c"));
  export_->add(
    pcap_writer_command, "pcap", "exports query results in PCAP format",
    opts()
      .add<std::string>("write,w", "path to write events to")
      .add<bool>("uds,d", "treat -w as UNIX domain socket to connect to")
      .add<size_t>("flush,f", "flush to disk after this many packets"));
#endif
}

command& default_application::import_cmd() {
  VAST_ASSERT(import_ != nullptr);
  return *import_;
}

command& default_application::export_cmd() {
  VAST_ASSERT(export_ != nullptr);
  return *export_;
}

} // namespace vast::system
