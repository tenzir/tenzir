#include "vast/configuration.h"

#include "config.h"

#include <iostream>
#include <string>
#include <boost/exception/diagnostic_information.hpp>
#include "vast/exception.h"
#include "vast/fs/path.h"
#include "vast/fs/fstream.h"
#include "vast/fs/operations.h"
#include "vast/logger.h"

namespace vast {

configuration::configuration()
  : visible_("")
  , all_("")
{
  po::options_description general("general options");
  general.add_options()
    ("config,c", po::value<fs::path>(), "configuration file")
    ("directory,d", po::value<fs::path>()->default_value("vast"),
     "VAST directory")
    ("help,h", "display this help")
    ("advanced,z", "show advanced options")
    ;

  po::options_description logger("logger options");
  logger.add_options()
    ("log.console-verbosity,v", po::value<int>()->default_value(logger::info),
     "console verbosity")
    ("log.file-verbosity,V", po::value<int>()->default_value(logger::verbose),
     "log file verbosity")
    ("log.directory", po::value<fs::path>()->default_value("log"),
     "log direcotory")
    ;

  po::options_description advanced("advanced options");
  advanced.add_options()
    ("profile,P", po::value<unsigned>(),
     "enable getrusage profiling at a given interval (seconds)")
#ifdef USE_PERFTOOLS_CPU_PROFILER
    ("profile-cpu", "also enable Google perftools CPU profiling")
#endif
#ifdef USE_PERFTOOLS_HEAP_PROFILER
    ("profile-heap", "also enable Google perftools heap profiling")
#endif
    ("broccoli-messages", "enable broccoli debug messages")
    ("broccoli-calltrace", "enable broccoli function call tracing")
    ;

  po::options_description actor("actor options");
  actor.add_options()
    ("all-server,a", "spawn all server components")
    ("ingestor-actor,I", "spawn the ingestor locally")
    ("archive-actor,A", "spawn the archive locally")
    ("index-actor,X", "spawn the index locally")
    ("search-actor,S", "spawn the search locally")
    ("tracker-actor,T", "spawn the ID tracker locally")
    ;

  po::options_description schema("schema options");
  schema.add_options()
    ("schema.file,s", po::value<std::string>(), "schema file")
    ("schema.print", "print the parsed event schema")
    ;

  po::options_description tracker("ID tracker options");
  tracker.add_options()
    ("tracker.host", po::value<std::string>()->default_value("127.0.0.1"),
     "hostname or IP address of the ID tracker")
    ("tracker.port", po::value<unsigned>()->default_value(42004),
     "TCP port of the ID tracker")
    ;

  po::options_description ingest("ingest options");
  ingest.add_options()
    ("ingest.max-events-per-chunk", po::value<size_t>()->default_value(1000),
     "maximum number of events per chunk")
    ("ingest.max-segment-size", po::value<size_t>()->default_value(1),
     "maximum segment size in MB")
    ("ingest.batch-size", po::value<size_t>()->default_value(1000),
     "number of events to ingest before yielding")
    ("ingest.broccoli-host", po::value<std::string>()->default_value("127.0.0.1"),
     "hostname or IP address of the broccoli source")
    ("ingest.broccoli-port", po::value<unsigned>()->default_value(42000),
     "port of the broccoli source")
    ("ingest.broccoli-events", po::value<std::vector<std::string>>()->multitoken(),
     "explicit list of events for broccoli to ingest")
    ("ingest.file-names", po::value<std::vector<std::string>>()->multitoken(),
     "file(s) to ingest")
    ("ingest.file-type", po::value<std::string>()->default_value("bro2"),
     "file type of the file(s) to ingest")
    ;

  po::options_description archive("archive options");
  archive.add_options()
    ("archive.host", po::value<std::string>()->default_value("127.0.0.1"),
     "hostname or IP address of the archive")
    ("archive.port", po::value<unsigned>()->default_value(42002),
     "port of the archive")
    ("archive.max-segments", po::value<size_t>()->default_value(500),
     "maximum number of segments to keep in memory")
    ;

  po::options_description index("index options");
  index.add_options()
    ("index.host", po::value<std::string>()->default_value("127.0.0.1"),
     "hostname or IP address of the index")
    ("index.port", po::value<unsigned>()->default_value(42003),
     "port of the index")
    ;

  po::options_description search("search options");
  search.add_options()
    ("search.host", po::value<std::string>()->default_value("127.0.0.1"),
     "hostname or IP address of the search")
    ("search.port", po::value<unsigned>()->default_value(42001),
     "port of the search")
    ;

  po::options_description client("client options");
  client.add_options()
    ("client.expression,e", po::value<std::string>(), "query expression")
    ("client.paginate,p", po::value<unsigned>()->default_value(10),
     "number of query results per page")
    ;

  all_.add(general).add(logger).add(advanced).add(actor).add(schema)
    .add(tracker).add(ingest).add(archive).add(index).add(search).add(client);

  visible_.add(general).add(actor);
}

bool configuration::load(std::string const& filename)
{
  try
  {
    if (! fs::exists(filename))
      return false;

    fs::ifstream ifs(filename);
    po::store(po::parse_config_file(ifs, all_), config_);

    init();
    return true;
  }
  catch (error::config const& e)
  {
    std::cerr << e.what() << std::endl;
  }
  catch (boost::program_options::unknown_option const& e)
  {
    std::cerr << e.what() << std::endl;
  }
  catch (boost::exception const& e)
  {
    std::cerr << boost::diagnostic_information(e);
  }

  return false;
}

bool configuration::load(int argc, char *argv[])
{
  try
  {
    po::store(parse_command_line(argc, argv, all_), config_);

    if (check("config"))
    {
      fs::path const& cfg = get<fs::path>("config");
      std::ifstream ifs(cfg.string().data());
      po::store(po::parse_config_file(ifs, all_), config_);
    }

    init();
    return true;
  }
  catch (error::config const& e)
  {
    std::cerr << e.what() << std::endl;
  }
  catch (boost::program_options::unknown_option const& e)
  {
    std::cerr << e.what() << ", try -h or --help" << std::endl;
  }
  catch (boost::program_options::invalid_command_line_syntax const& e)
  {
    std::cerr << "invalid command line: " << e.what() << std::endl;
  }
  catch (boost::exception const& e)
  {
    std::cerr << boost::diagnostic_information(e);
  }

  return false;
}

bool configuration::check(char const* option) const
{
  return config_.count(option);
}

void configuration::print(std::ostream& out, bool advanced) const
{
  out << 
    " _   _____   __________\n"
    "| | / / _ | / __/_  __/\n"
    "| |/ / __ |_\\ \\  / /\n"
    "|___/_/ |_/___/ /_/  " << VAST_VERSION << '\n'
    << (advanced ? all_ : visible_)
    << std::endl;
}

void configuration::init()
{
  po::notify(config_);

  depends("schema.print", "schema.file");

  auto cv = get<int>("log.console-verbosity");
  if (cv < 0 || cv > 6)
    throw error::config("verbosity not in [0,6]", "log.console-verbosity");

  auto fv = get<int>("log.file-verbosity");
  if (fv < 0 || fv > 6)
    throw error::config("verbosity not in [0,6]", "log.file-verbosity");

  if (check("profile") && get<unsigned>("profile") == 0)
    throw error::config("profiling interval must be non-zero", "profile");

  depends("client.paginate", "client.expression");
  conflicts("client.expression", "tracker-actor");
  conflicts("client.expression", "archive-actor");
  conflicts("client.expression", "index-actor");
  conflicts("client.expression", "search-actor");

  if (get<unsigned>("client.paginate") == 0)
    throw error::config("pagination must be non-zero", "client.paginate");

  auto log_dir = get<fs::path>("directory") / get<fs::path>("log.directory");
  auto log_file = log_dir / "vast.log";
  if (! fs::exists(log_dir))
      fs::mkdir(log_dir);

  logger::init(
      static_cast<logger::level>(cv),
      static_cast<logger::level>(fv),
      log_file);
}

void configuration::conflicts(const char* opt1, const char* opt2) const
{
  if (check(opt1) && ! config_[opt1].defaulted()
      && check(opt2) && ! config_[opt2].defaulted())
    throw error::config("conflicting options", opt1, opt2);
}

void configuration::depends(const char* for_what, const char* required) const
{
  if (check(for_what) && ! config_[for_what].defaulted() &&
      (! check(required) || config_[required].defaulted()))
    throw error::config("missing option dependency", for_what, required);
}

} // namespace vast
