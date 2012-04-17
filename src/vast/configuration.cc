#include "vast/configuration.h"

#include "config.h"

#include <iostream>
#include <string>
#include <thread>
#include "vast/exception.h"
#include "vast/fs/path.h"
#include "vast/fs/fstream.h"
#include "vast/fs/operations.h"
#include "vast/util/logger.h"

namespace vast {

configuration::configuration()
  : visible_("")
  , all_("")
{
    po::options_description general("general options");
    general.add_options()
        ("config,c", po::value<fs::path>(), "configuration file")
        ("vast-dir,d", po::value<fs::path>()->default_value("vast"),
         "VAST directory")
        ("help,h", "display this help")
        ("taxonomy,t", po::value<fs::path>(), "event taxonomy")
        ("query,q", po::value<std::string>(), "query expression")
        ("console-verbosity,v",
         po::value<int>()->default_value(static_cast<int>(log::info)),
         "console logging verbosity")
        ("advanced,z", "show advanced options")
    ;

    po::options_description advanced("advanced options");
    advanced.add_options()
        ("threads",
         po::value<unsigned>()->default_value(
             std::thread::hardware_concurrency()),
         "number of threads for asynchronous I/O")
        ("log-dir",
         po::value<fs::path>()->default_value(fs::path("vast") / "log"),
         "log directory")
        ("log-verbosity,V",
         po::value<int>()->default_value(static_cast<int>(log::verbose)),
         "log file verbosity")
        ("profile,p", "enable internal profiling")
        ("profiler-interval", po::value<unsigned>()->default_value(1000u),
         "profiling interval in milliseconds")
#ifdef USE_PERFTOOLS_CPU_PROFILER
        ("perftools-cpu", "enable Google perftools CPU profiling")
#endif
#ifdef USE_PERFTOOLS_HEAP_PROFILER
        ("perftools-heap", "enable Google perftools heap profiling")
#endif
        ("broccoli-messages", "enable broccoli debug messages")
        ("broccoli-calltrace", "enable broccoli function call tracing")
    ;

    po::options_description component("component options");
    component.add_options()
        ("comp-ingestor,I", "launch the ingestor")
        ("comp-archive,A", "launch the archive")
        ("comp-search,S", "launch the search")
    ;

    po::options_description taxonomy("taxonomy options");
    taxonomy.add_options()
        ("print-taxonomy,T", "print the parsed event taxonomy")
    ;

    po::options_description ingestor("ingestor options");
    ingestor.add_options()
        ("ingestor.host", po::value<std::string>()->default_value("127.0.0.1"),
         "IP address of the ingestor")
        ("ingestor.port", po::value<unsigned>()->default_value(42000u),
         "port of the ingestor")
        ("ingestor.events",
         po::value<std::vector<std::string>>()->multitoken(),
         "explicit list of events to ingest")
    ;

    po::options_description archive("archive options");
    archive.add_options()
        ("archive.max-events-per-chunk",
         po::value<size_t>()->default_value(1000u),
         "maximum number of events per chunk")
        ("archive.max-segment-size", po::value<size_t>()->default_value(1000u),
         "maximum segment size in KB")
        ("archive.max-segments", po::value<size_t>()->default_value(500u),
         "maximum number of segments to keep in memory")
    ;

    po::options_description search("search options");
    search.add_options()
        ("search.host", po::value<std::string>()->default_value("127.0.0.1"),
         "IP address of the search component")
        ("search.port", po::value<unsigned>()->default_value(42001u),
         "port of the search component")
    ;

    po::options_description client("client options");
    client.add_options()
        ("client.batch-size", po::value<unsigned>()->default_value(0u),
         "number of query results per page")
    ;

    all_.add(general).add(advanced).add(component).add(taxonomy)
        .add(ingestor).add(archive).add(search).add(client);

    visible_.add(general).add(component);
}

void configuration::load(std::string const& filename)
{
    if (fs::exists(filename))
    {
        fs::ifstream ifs(filename);
        po::store(po::parse_config_file(ifs, all_), config_);
    }

    init();
}

void configuration::load(int argc, char *argv[])
{
    po::store(parse_command_line(argc, argv, all_), config_);

    if (check("config"))
    {
        fs::path const& cfg = get<fs::path>("conf");
        std::ifstream ifs(cfg.string().c_str());
        po::store(po::parse_config_file(ifs, all_), config_);
    }

    init();
}

bool configuration::check(char const* option) const
{
    return config_.count(option);
}

void configuration::print(std::ostream& out, bool advanced) const
{
    out << " _   _____   __________\n"
           "| | / / _ | / __/_  __/\n"
           "| |/ / __ |_\\ \\  / /\n"
           "|___/_/ |_/___/ /_/  " << VAST_VERSION << '\n'
        << (advanced ? all_ : visible_)
        << std::endl;
}

void configuration::init()
{
    po::notify(config_);

    depends("dump-taxonomy", "taxonomy");

    int v = get<int>("console-verbosity");
    if (v < -1 || v > 5)
        throw config_exception("verbosity not in [0,5]", "console-verbosity");

    v = get<int>("log-verbosity");
    if (v < -1 || v > 5)
        throw config_exception("verbosity not in [0,5]", "log-verbosity");
}

void configuration::conflicts(const char* opt1, const char* opt2) const
{
    if (check(opt1) && ! config_[opt1].defaulted()
            && check(opt2) && ! config_[opt2].defaulted())
        throw config_exception("conflicting options", opt1, opt2);
}

void configuration::depends(const char* for_what, const char* required) const
{
    if (check(for_what) && ! config_[for_what].defaulted() &&
            (! check(required) || config_[required].defaulted()))
        throw config_exception("missing option dependency", for_what, required);
}

} // namespace vast
