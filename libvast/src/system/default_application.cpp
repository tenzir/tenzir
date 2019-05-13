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
#include "vast/format/json/suricata.hpp"
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

#define READER(name)                                                           \
  reader_command<format::name ::reader, defaults::import ::name>, #name

#define GENERATOR(name)                                                        \
  generator_command<format::name ::reader, defaults::import ::name>, #name

#define WRITER(name)                                                           \
  writer_command<format::name ::writer, defaults::export_ ::name>, #name

namespace vast::system {

default_application::default_application() {
  // Returns default options for commands.
  auto opts = [](std::string_view category = "global") {
    return command::opts(category);
  };
  // Set global options.
  root.options = opts("?system")
                   .add<std::string>("directory,d",
                                     "directory for persistent state")
                   .add<std::string>("endpoint,e", "node endpoint")
                   .add<std::string>("node-id,i", "the unique ID of this node")
                   .add<bool>("disable-accounting", "don't run the accountant")
                   .finish();
  // Add standalone commands.
  add(version_command, "version", "prints the software version", opts());
  add(start_command, "start", "starts a node", opts());
  add(remote_command, "stop", "stops a node", opts());
  add(remote_command, "spawn", "creates a new component", opts());
  add(remote_command, "kill", "terminates a component", opts());
  add(remote_command, "peer", "peers with another node", opts());
  add(remote_command, "status", "shows various properties of a topology",
      opts());
  auto send_cmd = add(remote_command, "send",
                      "sends a message to a registered actor", opts());
  send_cmd->visible = false;
  // Add "import" command and its children.
  import_ = add(nullptr, "import", "imports data from STDIN or file",
                opts("?import")
                  .add<caf::atom_value>("table-slice-type,t",
                                        "table slice type")
                  .add<bool>("node,N",
                             "spawn a node instead of connecting to one")
                  .add<bool>("blocking,b",
                             "block until the IMPORTER forwarded all data")
                  .add<size_t>("max-events,n",
                               "the maximum number of events to import"));
  import_->add(READER(zeek), "imports Zeek logs from STDIN or file",
               src_opts("?import.zeek"));
  import_->add(READER(mrt), "imports MRT logs from STDIN or file",
               src_opts("?import.mrt"));
  import_->add(READER(bgpdump), "imports BGPdump logs from STDIN or file",
               src_opts("?import.bgpdump"));
  namespace fj = format::json;
  import_->add(reader_command<fj::reader<>, defaults::import::json>, "json",
               "imports json with schema", src_opts("?import.json"));
  import_
    ->add(reader_command<fj::reader<fj::suricata>, defaults::import::suricata>,
          "suricata", "imports suricata eve json",
          src_opts("?import.suricata"));
  import_->add(GENERATOR(test),
               "imports random data for testing or benchmarking",
               opts("?import.test").add<size_t>("seed", "the random seed"));
  // Add "export" command and its children.
  export_ = add(nullptr, "export", "exports query results to STDOUT or file",
                opts("?export")
                  .add<bool>("node,N",
                             "spawn a node instead of connecting to one")
                  .add<bool>("continuous,c", "marks a query as continuous")
                  .add<bool>("historical,h", "marks a query as historical")
                  .add<bool>("unified,u", "marks a query as unified")
                  .add<size_t>("max-events,n", "maximum number of results")
                  .add<std::string>("read,r", "path for reading the query"));
  export_->add(WRITER(zeek), "exports query results in Zeek format",
               snk_opts("?export.zeek"));
  export_->add(WRITER(csv), "exports query results in CSV format",
               snk_opts("?export.csv"));
  export_->add(WRITER(ascii), "exports query results in ASCII format",
               snk_opts("?export.ascii"));
  export_->add(WRITER(json), "exports query results in JSON format",
               snk_opts("?export.json"));
  // Add PCAP import and export commands when compiling with PCAP enabled.
#ifdef VAST_HAVE_PCAP
  import_
    ->add(pcap_reader_command, "pcap", "imports PCAP logs from STDIN or file",
          opts("?import")
            .add<std::string>("read,r",
                              "path to input where to read events from")
            .add<std::string>("schema,s", "path to alternate schema")
            .add<bool>("uds,d", "treat -r as listening UNIX domain socket")
            .add<size_t>("cutoff,c", "skip flow packets after this many bytes")
            .add<size_t>("max-flows,m", "number of concurrent flows to track")
            .add<size_t>("max-flow-age,a", "max flow lifetime before eviction")
            .add<size_t>("flow-expiry,e", "flow table expiration interval")
            .add<size_t>("pseudo-realtime-factor,p",
                         "factor c delaying packets by 1/c"));
  export_->add(pcap_writer_command, "pcap",
               "exports query results in PCAP format",
               opts("?export")
                 .add<std::string>("write,w", "path to write events to")
                 .add<bool>("uds,d",
                            "treat -w as UNIX domain socket to connect to")
                 .add<size_t>("flush-interval,f",
                              "flush to disk after this many packets"));
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
