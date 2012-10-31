#include "vast/configuration.h"

#include <iostream> // FIXME: remove.
#include <ze/file_system.h>
#include "vast/logger.h"

namespace vast {

configuration::configuration()
{
  std::stringstream ss;
  ss << " _   _____   __________\n"
        "| | / / _ | / __/_  __/\n"
        "| |/ / __ |_\\ \\  / /\n"
        "|___/_/ |_/___/ /_/  " << VAST_VERSION;

  banner(ss.str());

  auto& general = create_block("general options");
  general.add('c', "config", "configuration file");
  general.add('h', "help", "display this help");
  general.add('d', "directory", "VAST directory").init("vast");
  general.add('z', "advanced", "show advanced options");

  auto& log = create_block("logger options", "log");
  log.add('v', "console-verbosity", "console verbosity").init(4);
  log.add('V', "file-verbosity", "log file verbosity").init(5);
  auto& advanced = create_block("advanced options");
  advanced.add('P', "profile", "enable getrusage profiling at a given interval (seconds)").single();
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
  actor.add('a', "all", "spawn all server actors");
  actor.add('I', "archive", "spawn the ingestor");
  actor.add('A', "index", "spawn the index");
  actor.add('X', "search", "spawn the search");
  actor.add('T', "tracker", "spawn the ID tracker");
  actor.visible(false);

  auto& schema = create_block("schema options", "schema");
  schema.add('s', "file", "schema file").single();
  schema.add("print", "print the parsed event schema");
  schema.visible(false);

  auto& tracker = create_block("ID tracker options", "tracker");
  tracker.add("host", "hostname/address of the tracker").init("127.0.0.1");
  tracker.add("port", "TCP port of the ID tracker").init(42004);
  tracker.visible(false);

  auto& ingest = create_block("ingest options", "ingest");
  ingest.add("max-events-per-chunk", "maximum number of events per chunk").init(1000);
  ingest.add("max-segment-size", "maximum segment size in MB").init("1");
  ingest.add("batch-size", "number of events to ingest in one run").init(4000);
  ingest.add("file-names", "file(s) to ingest").multi();
  ingest.add("file-type", "file type of the file(s) to ingest").init("bro2");
#ifdef VAST_HAVE_BROCCOLI
  ingest.add("broccoli-host", "hostname/address of the broccoli source").init("127.0.0.1");
  ingest.add("broccoli-port", "port of the broccoli source").init(42000);
  ingest.add("broccoli-events", "list of events for broccoli to subscribe to").multi();
#endif
  ingest.visible(false);

  auto& archive = create_block("archive options", "archive");
  archive.add("host", "hostname/address of the archive").init("127.0.0.1");
  archive.add("port", "TCP port of the archive").init(42002);
  archive.add("max-segments", "maximum number of segments to keep in memory").init(500);
  archive.visible(false);

  auto& index = create_block("index options", "index");
  index.add("host", "hostname/address of the archive").init("127.0.0.1");
  index.add("port", "TCP port of the index").init(42003);
  index.visible(false);

  auto& search = create_block("search options", "search");
  search.add("host", "hostname/address of the archive").init("127.0.0.1");
  search.add("port", "TCP port of the search").init(42001);
  search.visible(false);

  auto& client = create_block("client options", "client");
  client.add("expression", "query expression").single();
  client.add("paginate", "number of query results per page").init(10);
  client.visible(false);
}

void configuration::verify()
{
  depends("schema.print", "schema.file");

  auto cv = as<int>("log.console-verbosity");
  if (cv < 0 || cv > 6)
    throw error::config("verbosity not in [0,6]", "log.console-verbosity");

  auto fv = as<int>("log.file-verbosity");
  if (fv < 0 || fv > 6)
    throw error::config("verbosity not in [0,6]", "log.file-verbosity");

  if (check("profile") && as<unsigned>("profile") == 0)
    throw error::config("profiling interval must be non-zero", "profile");

  depends("client.paginate", "client.expression");
  conflicts("client.expression", "tracker-actor");
  conflicts("client.expression", "archive-actor");
  conflicts("client.expression", "index-actor");
  conflicts("client.expression", "search-actor");

  if (as<unsigned>("client.paginate") == 0)
    throw error::config("pagination must be non-zero", "client.paginate");

  auto log_dir = ze::path(get("directory")) / "log";
  if (! ze::exists(log_dir))
      ze::mkdir(log_dir);

  logger::init(
      static_cast<logger::level>(cv),
      static_cast<logger::level>(fv),
      log_dir / "vast.log");
}

} // namespace vast
