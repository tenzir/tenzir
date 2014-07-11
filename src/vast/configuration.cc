#include "vast/configuration.h"
#include "vast/file_system.h"
#include "vast/logger.h"
#include "vast/string.h"
#include "vast/type_info.h"
#include "vast/detail/type_manager.h"
#include "vast/util/color.h"

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
  general.add('c', "config", "configuration file");
  general.add('h', "help", "display this help");
  general.add('d', "directory", "VAST directory").init("vast");
  general.add('z', "advanced", "show advanced options");
  general.add("version", "print the version of VAST");

  auto min = 0;
  auto max = VAST_LOG_LEVEL;
  auto range = '[' + std::to_string(min) + '-' + std::to_string(max) + ']';

  auto& log = create_block("logger options", "log");
  log.add('v', "console-verbosity", "console verbosity " + range)
     .init(std::min(3, max));
  log.add('V', "file-verbosity", "log file verbosity " + range)
     .init(std::min(4, max));
  log.add("no-colors", "don't use colors for console output");
  log.add("function-names", "log function names");

  auto& advanced = create_block("advanced options");
  advanced.add('P', "profile",
               "enable getrusage profiling at a given interval (seconds)")
          .single();
#ifdef VAST_USE_PERFTOOLS_CPU_PROFILER
  advanced.add("profile-cpu", "also enable Google perftools CPU profiling");
#endif
#ifdef VAST_USE_PERFTOOLS_HEAP_PROFILER
  advanced.add("profile-heap", "also enable Google perftools heap profiling");
#endif
  advanced.visible(false);

  auto& act = create_block("actor options");
  act.add('a', "all-core", "spawn all core actors");
#ifdef VAST_HAVE_EDITLINE
  act.add('C', "console-actor", "spawn the console client actor");
#endif
  act.add('A', "archive-actor", "spawn the archive");
  act.add('I', "importer-actor", "spawn the importer");
  act.add('E', "exporter-actor", "spawn the exporter");
  act.add('R', "receiver-actor", "spawn the receiver");
  act.add('S', "search-actor", "spawn the search");
  act.add('T', "tracker-actor", "spawn the ID tracker");
  act.add('X', "index-actor", "spawn the index");
  act.visible(false);

  auto& imp = create_block("import options", "import");
  imp.add("max-events-per-chunk", "maximum events per chunk").init(5000);
  imp.add("max-segment-size", "maximum segment size in MB").init(128);
  imp.add("batch-size", "number of events to ingest in one run").init(4000);
  imp.add('r', "read", "path to input file/directory").init("-");
  imp.add('i', "format", "format of events to ingest").init("bro");
  imp.add("submit", "send orphaned segments on startup");
  imp.visible(false);

  auto& exp = create_block("export options", "export");
  exp.add('l', "limit", "maximum number of results").init(0);
  exp.add('o', "format", "format of events to generate").init("bro");
  exp.add('q', "query", "the query string").single();
  exp.add('w', "write", "path to output file/directory").init("-");
  exp.visible(false);

  auto& recv = create_block("receiver options", "receiver");
  recv.add("host", "hostname/address of the receiver").init("127.0.0.1");
  recv.add("port", "TCP port of the receiver").init(42000);
  recv.visible(false);

  auto& archive = create_block("archive options", "archive");
  archive.add("host", "hostname/address of the archive").init("127.0.0.1");
  archive.add("port", "TCP port of the archive").init(42003);
  archive.add("max-segments", "maximum segments cached in memory").init(10);
  archive.visible(false);

  auto& idx = create_block("index options", "index");
  idx.add("host", "hostname/address of the archive").init("127.0.0.1");
  idx.add("port", "TCP port of the index").init(42004);
  idx.add('p', "partition", "name of the partition to append to").single();
  idx.add("batch-size", "number of events to index in one run").init(1000);
  idx.add("rebuild", "rebuild indexes from archive");
  idx.visible(false);

  auto& track = create_block("ID tracker options", "tracker");
  track.add("host", "hostname/address of the tracker").init("127.0.0.1");
  track.add("port", "TCP port of the ID tracker").init(42002);
  track.visible(false);

  auto& search = create_block("search options", "search");
  search.add("host", "hostname/address of the archive").init("127.0.0.1");
  search.add("port", "TCP port of the search").init(42001);
  search.visible(false);

#ifdef VAST_HAVE_EDITLINE
  add_conflict("console-actor", "all-core");
  add_conflict("console-actor", "tracker-actor");
  add_conflict("console-actor", "archive-actor");
  add_conflict("console-actor", "index-actor");
  add_conflict("console-actor", "importer-actor");
  add_conflict("console-actor", "exporter-actor");
  add_conflict("console-actor", "search-actor");
  add_conflict("console-actor", "receiver-actor");
#endif

  add_dependency("import.submit", "importer-actor");
  add_dependency("import.read", "importer-actor");
  add_dependency("import.format", "import.read");

  add_dependencies("index.partition", {"index-actor", "all-core"});
  add_conflict("index.rebuild", "index.partition");

  add_conflict("importer-actor", "exporter-actor");
  add_conflict("receiver-actor", "exporter-actor");
  add_conflict("tracker-actor", "exporter-actor");

  add_dependency("export.limit", "exporter-actor");
  add_dependency("export.format", "exporter-actor");
  add_dependency("export.query", "exporter-actor");
  add_dependency("export.write", "exporter-actor");
}

} // namespace vast
