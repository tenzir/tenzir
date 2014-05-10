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
#ifdef VAST_HAVE_BROCCOLI
  advanced.add("broccoli-messages", "enable broccoli debug messages");
  advanced.add("broccoli-calltrace", "enable broccoli function call tracing");
#endif
  advanced.visible(false);

  auto& actor = create_block("actor options");
#ifdef VAST_HAVE_EDITLINE
  actor.add('C', "console-actor", "spawn the console client actor");
#endif
  actor.add('a', "all-server", "spawn all server actors");
  actor.add('A', "archive-actor", "spawn the archive");
  actor.add('I', "ingestor-actor", "spawn the ingestor");
  actor.add('R', "receiver-actor", "spawn the receiver");
  actor.add('S', "search-actor", "spawn the search");
  actor.add('T', "tracker-actor", "spawn the ID tracker");
  actor.add('X', "index-actor", "spawn the index");
  actor.visible(false);

  auto& ingest = create_block("ingest options", "ingest");
  ingest.add("max-events-per-chunk", "maximum number of events per chunk")
        .init(5000);
  ingest.add("max-segment-size", "maximum segment size in MB").init(128);
  ingest.add("batch-size", "number of events to ingest in one run").init(4000);
  ingest.add('r', "file-name", "path to file to ingest").single();
  ingest.add("file-type", "file type of the file to ingest").init("bro2");
  ingest.add("time-field", "field to extract event timestamp from").init(-1);
  ingest.add("submit", "send orphaned segments on startup");
#ifdef VAST_HAVE_BROCCOLI
  ingest.add("broccoli-host", "hostname/address of the broccoli source")
        .init("127.0.0.1");
  ingest.add("broccoli-port", "port of the broccoli source").init(42000);
  ingest.add("broccoli-events", "list of events for broccoli to subscribe to")
        .multi();
#endif
  ingest.visible(false);

  auto& receiver = create_block("receiver options", "receiver");
  receiver.add("host", "hostname/address of the receiver").init("127.0.0.1");
  receiver.add("port", "TCP port of the receiver").init(42000);
  receiver.visible(false);

  auto& archive = create_block("archive options", "archive");
  archive.add("host", "hostname/address of the archive").init("127.0.0.1");
  archive.add("port", "TCP port of the archive").init(42003);
  archive.add("max-segments", "maximum number of segments to keep in memory")
         .init(500);
  archive.visible(false);

  auto& index = create_block("index options", "index");
  index.add("host", "hostname/address of the archive").init("127.0.0.1");
  index.add("port", "TCP port of the index").init(42004);
  index.add("partition", "name of the partition to append to").single();
  index.add("batch-size", "number of events to index in one run").init(1000);
  index.add("rebuild", "rebuild indexes from archive");
  index.visible(false);

  auto& tracker = create_block("ID tracker options", "tracker");
  tracker.add("host", "hostname/address of the tracker").init("127.0.0.1");
  tracker.add("port", "TCP port of the ID tracker").init(42002);
  tracker.visible(false);

  auto& search = create_block("search options", "search");
  search.add("host", "hostname/address of the archive").init("127.0.0.1");
  search.add("port", "TCP port of the search").init(42001);
  search.visible(false);

#ifdef VAST_HAVE_EDITLINE
  add_conflict("console-actor", "all-server");
  add_conflict("console-actor", "tracker-actor");
  add_conflict("console-actor", "archive-actor");
  add_conflict("console-actor", "index-actor");
  add_conflict("console-actor", "ingestor-actor");
  add_conflict("console-actor", "search-actor");
  add_conflict("console-actor", "receiver-actor");
#endif

  add_dependency("ingest.time-field", "ingestor-actor");
  add_dependency("ingest.submit", "ingestor-actor");
  add_dependency("ingest.file-name", "ingestor-actor");
  add_dependency("ingest.file-type", "ingest.file-name");
  add_dependencies("index.partition", {"index-actor", "all-server"});
  add_conflict("index.rebuild", "index.partition");
}

} // namespace vast
