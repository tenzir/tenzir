#include "vast/configuration.h"

#include "vast/filesystem.h"
#include "vast/logger.h"
#include "vast/uuid.h"
#include "vast/time.h"
#include "vast/detail/type_manager.h"
#include "vast/util/color.h"
#include "vast/util/system.h"

namespace vast {

std::string configuration::banner() const
{
  std::stringstream ss;
  auto colorize = ! check("log.no-colors");
  if (colorize)
    ss << util::color::red;

  ss << "     _   _____   __________\n"
        "    | | / / _ | / __/_  __/\n"
        "    | |/ / __ |_\\ \\  / /\n"
        "    |___/_/ |_/___/ /_/  ";

  if (colorize)
    ss << util::color::yellow;

  ss << VAST_VERSION;

  if (colorize)
    ss << util::color::reset;

  return ss.str();
}

void configuration::initialize()
{
  auto& general = create_block("general options");
  general.add('h', "help", "display this help");
  general.add('d', "directory", "VAST directory").init("vast");
  general.add('z', "advanced", "show advanced options");
  general.add("version", "print the version of VAST");

  auto& caf = create_block("CAF options", "caf");
  caf.add("threads", "number of worker threads in scheduler").single();
  caf.add("throughput", "maximum number of messages per worker invocation").single();
  caf.visible(false);

  auto min = 0;
  auto max = VAST_LOG_LEVEL;
  auto range = '(' + std::to_string(min) + '-' + std::to_string(max) + ')';

  auto hostname = util::hostname();
  if (! hostname || hostname->empty())
    hostname = to_string(uuid::random()).substr(0, 6);  // FIXME: uniform?

  auto time_pid =
    std::to_string(time::now().since_epoch().seconds()) +
    '_' + std::to_string(util::process_id());

  auto& log = create_block("logging options", "log");
  log.add("directory", "log directory relative to base").init("log/" + time_pid);
  log.add('v', "console", "console verbosity " + range).init(std::min(3, max));
  log.add('V', "file", "log file verbosity " + range).init(std::min(4, max));
  log.add("no-colors", "don't use colors for console output");
  log.add("function-names", "log function names");

  auto& act = create_block("actor options");
  act.add('C', "core", "spawn all core actors (-S -T -R -A -X)");
  act.add('T', "tracker", "spawn a tracker");
  act.add('R', "receiver", "spawn a receiver");
  act.add('A', "archive", "spawn an archive");
  act.add('X', "index", "spawn an index");
  act.add('S', "search", "spawn a search");
  act.add('E', "exporter", "spawn an exporter").single();
  act.add('I', "importer", "spawn an importer").single();
  act.add('Q', "console", "spawn a query console");

  auto& track = create_block("tracker options", "tracker");
  track.add("host", "hostname/address of the tracker").init("127.0.0.1");
  track.add("port", "TCP port of the tracker").init(42000);
  track.add("link", "link two components").multi(2);
  track.visible(false);

  auto& imp = create_block("import options", "import");
  imp.add('s', "schema", "the schema to use for the generated events").single();
  imp.add('r', "read", "path to input file/directory").init("-");
  imp.add('i', "interface", "name of interface to read packets from").single();
  imp.add("compression", "the compression method for chunks").init("lz4");
  imp.add('n', "chunk-size", "number of events to ingest in one run").init(8192);
  imp.add("sniff-schema", "print the log schema and exit");
  imp.add("pcap-cutoff", "forego intra-flow packets after this many bytes").single();
  imp.add("pcap-flow-max", "number of concurrent flows to track").init(1000000);
  imp.add("pcap-flow-age", "maximum flow lifetime before eviction").init(60);
  imp.add("pcap-flow-expiry", "flow table expiration interval").init(10);
  imp.add("pcap-pseudo-realtime", "factor c delaying packets in trace by 1/c").init(0);
  imp.add("test-id", "the base event ID").init(0);
  imp.add("test-events", "number of events to generate").init(100);
  imp.add("name", "default importer name").init("importer@" + *hostname);
  imp.visible(false);

  auto& exp = create_block("export options", "export");
  exp.add("schema", "the schema to use for the generated events").single();
  exp.add('c', "continuous", "marks a query as continuous");
  exp.add('l', "limit", "maximum number of results").init(0);
  exp.add('e', "expression", "the query expression").single();
  exp.add('q', "historical", "marks a query as historical");
  exp.add('u', "unified", "marks a query as both historical and continuous");
  exp.add('w', "write", "path to output file/directory").init("-");
  exp.add("pcap-flush", "flush to disk after this many packets").init(10000);
  exp.add("name", "default exporter name").init("exporter@" + *hostname);
  exp.visible(false);

  auto& recv = create_block("receiver options", "receiver");
  recv.add("name", "default receiver name").init("receiver@" + *hostname);
  recv.visible(false);

  auto& arch = create_block("archive options", "archive");
  arch.add("max-segment-size", "maximum segment size in MB").init(128);
  arch.add("max-segments", "maximum segments cached in memory").init(10);
  arch.add("name", "default archive name").init("archive@" + *hostname);
  arch.visible(false);

  auto& idx = create_block("index options", "index");
  idx.add('p', "part-size", "maximum events per partition").init(1 << 20);
  idx.add('m', "part-max", "maximum number of partitions in memory").init(10);
  idx.add('a', "part-active", "number of active partitions").init(5);
  idx.add("rebuild", "delete and rebuild index from archive");
  idx.add("name", "default index name").init("index@" + *hostname);
  idx.visible(false);

  auto& srch = create_block("search options", "search");
  srch.add("name", "default search name").init("search@" + *hostname);
  srch.visible(false);

  auto& prof = create_block("profiler options", "profiler");
  prof.add("interval", "profiling granularity in seconds").init(1);
  prof.add("rusage", "enable rusage profiling");
  prof.add("cpu", "enable gperftools CPU profiling");
  prof.add("heap", "enable gperftools heap profiling");
  prof.visible(false);

  add_conflict("console", "core");
  add_conflict("console", "tracker");
  add_conflict("console", "archive");
  add_conflict("console", "index");
  add_conflict("console", "importer");
  add_conflict("console", "exporter");
  add_conflict("console", "search");
  add_conflict("console", "receiver");

  add_dependency("import.schema", "importer");
  add_dependency("import.read", "importer");
  add_dependency("import.interface", "importer");
  add_dependency("import.sniff-schema", "importer");
  add_dependency("import.pcap-cutoff", "importer");
  add_dependency("import.pcap-maxflows", "importer");
  add_conflict("import.read", "import.interface");
  add_conflict("import.schema", "import.sniff-schema");

  add_dependency("export.limit", "exporter");
  add_dependency("export.expression", "exporter");
  add_dependencies("export.expression",
                   {"export.historical", "export.continuous", "export.unified"});
  add_dependency("exporter", "export.expression");
  add_dependency("export.write", "exporter");
  add_dependency("export.pcap-flush", "exporter");
  add_conflict("importer", "exporter");
  add_conflict("receiver", "exporter");
  add_conflict("tracker", "exporter");
}

} // namespace vast
