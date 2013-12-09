#include "vast/configuration.h"
#include "vast/file_system.h"
#include "vast/logger.h"
#include "vast/string.h"
#include "vast/type_info.h"
#include "vast/detail/type_manager.h"

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
  log.add('v', "console-verbosity", "console verbosity").init(3);
  log.add('V', "file-verbosity", "log file verbosity").init(4);
  log.add("function-names", "log function names");

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
#ifdef VAST_HAVE_EDITLINE
  actor.add('C', "console-actor", "spawn the console client actor");
#endif
  actor.add('a', "all-server", "spawn all server actors");
  actor.add('A', "archive-actor", "spawn the archive");
  actor.add('I', "ingestor-actor", "spawn the ingestor");
  actor.add('X', "index-actor", "spawn the index");
  actor.add('S', "search-actor", "spawn the search");
  actor.add('T', "tracker-actor", "spawn the ID tracker");
  actor.visible(false);

  auto& schema = create_block("schema options", "schema");
  schema.add('s', "file", "schema file").single();
  schema.add("print", "print the parsed event schema");
  schema.visible(false);

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

  auto& receiver = create_block("receiver options", "receiver");
  receiver.add("host", "hostname/address of the receiver").init("127.0.0.1");
  receiver.add("port", "TCP port of the receiver").init(42000);
  receiver.visible(false);

  auto& archive = create_block("archive options", "archive");
  archive.add("host", "hostname/address of the archive").init("127.0.0.1");
  archive.add("port", "TCP port of the archive").init(42003);
  archive.add("max-segments", "maximum number of segments to keep in memory").init(500);
  archive.visible(false);

  auto& index = create_block("index options", "index");
  index.add("host", "hostname/address of the archive").init("127.0.0.1");
  index.add("port", "TCP port of the index").init(42004);
  index.visible(false);

  auto& tracker = create_block("ID tracker options", "tracker");
  tracker.add("host", "hostname/address of the tracker").init("127.0.0.1");
  tracker.add("port", "TCP port of the ID tracker").init(42002);
  tracker.visible(false);

  auto& search = create_block("search options", "search");
  search.add("host", "hostname/address of the archive").init("127.0.0.1");
  search.add("port", "TCP port of the search").init(42001);
  search.visible(false);
}

void configuration::verify()
{
  depends("schema.print", "schema.file");

  if (check("profile") && as<unsigned>("profile") == 0)
    throw error::config("profiling interval must be non-zero", "profile");

#ifdef VAST_HAVE_EDITLINE
  conflicts("console-actor", "all-server");
  conflicts("console-actor", "tracker-actor");
  conflicts("console-actor", "archive-actor");
  conflicts("console-actor", "index-actor");
  conflicts("console-actor", "ingestor-actor");
  conflicts("console-actor", "search-actor");
#endif
}

bool initialize(configuration const& config)
{
  path vast_dir = string(config.get("directory"));
  if (! exists(vast_dir))
    if (! mkdir(vast_dir))
      return false;

  logger::instance()->init(
      static_cast<logger::level>(config.as<uint32_t>("log.console-verbosity")),
      static_cast<logger::level>(config.as<uint32_t>("log.file-verbosity")),
      config.check("log.function-names"),
      vast_dir / "log");

  VAST_LOG_VERBOSE(" _   _____   __________");
  VAST_LOG_VERBOSE("| | / / _ | / __/_  __/");
  VAST_LOG_VERBOSE("| |/ / __ |_\\ \\  / / ");
  VAST_LOG_VERBOSE("|___/_/ |_/___/ /_/  " << VAST_VERSION);
  VAST_LOG_VERBOSE("");

  announce_builtin_types();

  size_t n = 0;
  detail::type_manager::instance()->each([&](global_type_info const&) { ++n; });
  VAST_LOG_DEBUG("type manager announced " << n << " types");

  return true;
}

void shutdown()
{
  std::atomic_thread_fence(std::memory_order_seq_cst);
  detail::type_manager::destruct();
  logger::destruct();
}

} // namespace vast
