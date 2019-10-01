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

#include "vast/system/application.hpp"

#include "vast/command.hpp"
#include "vast/config.hpp"
#include "vast/detail/assert.hpp"
#include "vast/format/ascii.hpp"
#include "vast/format/bgpdump.hpp"
#include "vast/format/csv.hpp"
#include "vast/format/json.hpp"
#include "vast/format/json/suricata.hpp"
#include "vast/format/mrt.hpp"
#include "vast/format/test.hpp"
#include "vast/format/zeek.hpp"
#include "vast/system/configuration.hpp"
#include "vast/system/generator_command.hpp"
#include "vast/system/reader_command.hpp"
#include "vast/system/remote_command.hpp"
#include "vast/system/start_command.hpp"
#include "vast/system/version_command.hpp"
#include "vast/system/writer_command.hpp"

#ifdef VAST_HAVE_PCAP
#  include "vast/system/pcap_reader_command.hpp"
#  include "vast/system/pcap_writer_command.hpp"
#endif

#define READER(name)                                                           \
  reader_command<format::name ::reader, defaults::import ::name>

#define GENERATOR(name)                                                        \
  generator_command<format::name ::reader, defaults::import ::name>

#define WRITER(name)                                                           \
  writer_command<format::name ::writer, defaults::export_ ::name>

namespace {

/// @returns default options for source commands.
auto src_opts(std::string_view category) {
  return vast::command::opts(category)
    .add<std::string>("listen,l", "the port number to listen on")
    .add<std::string>("read,r", "path to input where to read events from")
    .add<std::string>("schema-file,s", "path to alternate schema")
    .add<std::string>("schema,S", "alternate schema as string")
    .add<std::string>("type,t", "type the data should be parsed as")
    .add<bool>("uds,d", "treat -r as listening UNIX domain socket");
}

// @returns defaults options for sink commands.
auto snk_opts(std::string_view category) {
  return vast::command::opts(category)
    .add<std::string>("write,w", "path to write events to")
    .add<bool>("uds,d", "treat -w as UNIX domain socket to connect to");
}

} // namespace

namespace vast::system {

vast::command make_application() {
  using caf::atom_value;
  // Returns default options for commands.
  auto opts = [](std::string_view category = "global") {
    return command::opts(category);
  };
  // Set global options.
  // clang-format off
  const char* schema_paths_help =
    "list of paths to look for schema files"
    " ([" VAST_INSTALL_PREFIX "/share/vast/schema])";
  // clang-format on
  command root;
  root.options
    = opts("?system")
        .add<std::string>("config-file", "path to a configuration file")
        .add<atom_value>("verbosity", "output verbosity level on the console")
        .add<std::vector<std::string>>("schema-paths", schema_paths_help)
        .add<std::string>("directory,d", "directory for persistent state")
        .add<std::string>("endpoint,e", "node endpoint")
        .add<std::string>("node-id,i", "the unique ID of this node")
        .add<bool>("disable-accounting", "don't run the accountant")
        .add<bool>("no-default-schema", "don't load the default schema "
                                        "definitions")
        .finish();
  // Add standalone commands.
  root.add("version", opts())
    ->describe("prints the software version")
    ->run(version_command);
  root.add("start", opts())->describe("starts a node")->run(start_command);
  root.add("stop", opts())->describe("stops a node")->run(remote_command);
  root.add("spawn", opts())
    ->describe("creates a new component")
    ->run(remote_command);
  root.add("kill", opts())
    ->describe("terminates a component")
    ->run(remote_command);
  root.add("peer", opts())
    ->describe("peers with another node")
    ->run(remote_command);
  root.add("status", opts())
    ->describe("shows various properties of a topology")
    ->run(remote_command);
  root.add("send", opts())
    ->describe("sends a message to a registered actor")
    ->run(remote_command)
    ->hide();
  // Add "import" command and its children.
  auto import_
    = root
        .add("import",
             opts("?import")
               .add<atom_value>("table-slice-type,t", "table slice type")
               .add<bool>("node,N", "spawn a node instead of connecting to one")
               .add<bool>("blocking,b", "block until the IMPORTER forwarded "
                                        "all "
                                        "data")
               .add<size_t>("max-events,n", "the maximum number of events to "
                                            "import"))
        ->describe("imports data from STDIN or file");
  import_->add("zeek", src_opts("?import.zeek"))
    ->describe("imports Zeek logs from STDIN or file")
    ->run(READER(zeek));
  import_->add("mrt", src_opts("?import.mrt"))
    ->describe("imports MRT logs from STDIN or file")
    ->run(READER(mrt));
  import_->add("bgpdump", src_opts("?import.bgpdump"))
    ->describe("imports BGPdump logs from STDIN or file")
    ->run(READER(bgpdump));
  import_->add("csv", src_opts("?import.csv"))
    ->describe("imports CSV logs from STDIN or file")
    ->run(READER(csv));
  namespace fj = format::json;
  import_->add("json", src_opts("?import.json"))
    ->describe("imports json with schema")
    ->run(reader_command<fj::reader<>, defaults::import::json>);
  import_->add("suricata", src_opts("?import.suricata"))
    ->describe("imports suricata eve json")
    ->run(reader_command<fj::reader<fj::suricata>, defaults::import::suricata>);
  import_
    ->add("test", opts("?import.test")
                    .add<size_t>("seed", "the random seed")
                    .add<std::string>("schema-file,s", "path to alternate "
                                                       "schema")
                    .add<std::string>("schema,S", "alternate schema as string"))
    ->describe("imports random data for testing or benchmarking")
    ->run(GENERATOR(test));
  // Add "export" command and its children.
  auto export_
    = root
        .add("export",
             opts("?export")
               .add<bool>("node,N", "spawn a node instead of connecting "
                                    "to one")
               .add<bool>("continuous,c", "marks a query as continuous")
               .add<bool>("historical,h", "marks a query as historical")
               .add<bool>("unified,u", "marks a query as unified")
               .add<size_t>("max-events,n", "maximum number of results")
               .add<std::string>("read,r", "path for reading the query"))
        ->describe("exports query results to STDOUT or file");
  export_->add("zeek", snk_opts("?export.zeek"))
    ->describe("exports query results in Zeek format")
    ->run(WRITER(zeek));
  export_->add("csv", snk_opts("?export.csv"))
    ->describe("exports query results in CSV format")
    ->run(WRITER(csv));
  export_->add("ascii", snk_opts("?export.ascii"))
    ->describe("exports query results in ASCII format")
    ->run(WRITER(ascii));
  export_->add("json", snk_opts("?export.json"))
    ->describe("exports query results in JSON format")
    ->run(WRITER(json));
  // Add PCAP import and export commands when compiling with PCAP enabled.
#ifdef VAST_HAVE_PCAP
  import_
    ->add("pcap",
          opts("?import")
            .add<std::string>("read,r", "path to input where to read events "
                                        "from")
            .add<std::string>("schema,s", "path to alternate schema")
            .add<bool>("uds,d", "treat -r as listening UNIX domain socket")
            .add<size_t>("cutoff,c", "skip flow packets after this many bytes")
            .add<size_t>("max-flows,m", "number of concurrent flows to track")
            .add<size_t>("max-flow-age,a", "max flow lifetime before eviction")
            .add<size_t>("flow-expiry,e", "flow table expiration interval")
            .add<size_t>("pseudo-realtime-factor,p", "factor c delaying "
                                                     "packets by 1/c"))
    ->describe("imports PCAP logs from STDIN or file")
    ->run(pcap_reader_command);
  export_
    ->add("pcap",
          opts("?export")
            .add<std::string>("write,w", "path to write events to")
            .add<bool>("uds,d", "treat -w as UNIX domain socket to connect to")
            .add<size_t>("flush-interval,f", "flush to disk after this many "
                                             "packets"))
    ->describe("exports query results in PCAP format")
    ->run(pcap_writer_command);
#endif
  return root;
}

void render_error(const command& root, const caf::error& err,
                  std::ostream& os) {
  if (!err)
    // The user most likely killed the process via CTRL+C, print nothing.
    return;
  os << render(err);
  if (err.category() == caf::atom("vast")) {
    auto x = static_cast<vast::ec>(err.code());
    switch (x) {
      default:
        break;
      case ec::invalid_subcommand:
      case ec::missing_subcommand:
      case ec::unrecognized_option: {
        auto ctx = err.context();
        if (ctx.match_element<std::string>(1)) {
          auto name = ctx.get_as<std::string>(1);
          if (auto cmd = resolve(root, name))
            helptext(*cmd, os);
        }
        else {
          VAST_ASSERT("User visible error contexts must consist of strings!");
        }
        break;
      }
    }
  }
}

} // namespace vast::system
