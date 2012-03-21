#include "core/configuration.h"

#include "config.h"

#include <iostream>
#include <fstream>
#include <string>
#include <thread>
#include "core/error.h"
#include "fs/path.h"
#include "util/logger.h"

namespace vast {
namespace core {

configuration::configuration()
  : visible_("")
  , all_("available options")
{
}

void configuration::init(int argc, char *argv[])
{
    po::options_description general("general options");
    general.add_options()
        ("config,c", po::value<fs::path>(), "configuration file")
        ("dir,d", po::value<fs::path>()->default_value("vast"),
         "VAST directory")
        ("help,h", "display this help")
        ("taxonomy,t", po::value<fs::path>(), "event taxonomy")
        ("console-verbosity,v",
         po::value<int>()->default_value(static_cast<int>(log::info)),
         "console logging verbosity")
        ("advanced,z", "show advanced options")
    ;

    po::options_description advanced("advanced options");
    advanced.add_options()
        ("log-dir",
         po::value<fs::path>()->default_value("log"), "log directory")
        ("log-verbosity,V",
         po::value<int>()->default_value(static_cast<int>(log::verbose)),
         "log file verbosity")
        ("profile,p", "enable internal profiling")
        ("profiler-interval", po::value<unsigned>()->default_value(1000U),
         "profiling interval in milliseconds")
#ifdef USE_PERFTOOLS_CPU_PROFILER
        ("perftools-cpu", "enable Google perftools CPU profiling")
#endif
#ifdef USE_PERFTOOLS_HEAP_PROFILER
        ("perftools-heap", "enable Google perftools heap profiling")
#endif
    ;

    po::options_description component("component options");
    component.add_options()
        ("all,a", "launch all components")
        ("ingestor,I", "launch the ingestor")
        ("database,D", "create a database locally")
        ("query-manager,Q", "launch the query manager")
    ;

    po::options_description taxonomy("taxonomy options");
    taxonomy.add_options()
        ("print-taxonomy,T", "print the parsed event taxonomy")
    ;

    po::options_description ingest("ingest options");
    ingest.add_options()
        ("ingest.ip", po::value<std::string>()->default_value("127.0.0.1"),
         "IP address of the ingestor")
        ("ingest.port", po::value<unsigned>()->default_value(42000),
         "port of the ingestor")
    ;

    all_.add(general).add(advanced).add(component).add(taxonomy).add(ingest);

    visible_.add(general).add(component);

    po::store(parse_command_line(argc, argv, all_), config_);

    if (check("config"))
    {
        fs::path const& cfg = get<fs::path>("conf");
        std::ifstream ifs(cfg.string().c_str());
        po::store(po::parse_config_file(ifs, all_), config_);
    }

    po::notify(config_);

    conflicts("all", "dispatcher");
    conflicts("all", "database");
    conflicts("all", "query-manager");

    depends("all", "taxonomy");
    depends("database", "taxonomy");
    depends("dispatcher", "taxonomy");
    depends("dump-taxonomy", "taxonomy");

    int v = get<int>("console-verbosity");
    if (v < -1 || v > 5)
    {
        std::cerr << "verbosity only takes values between 0 and 5" << std::endl;
        THROW(error::config() << error::option("console-verbosity"));
    }

    v = get<int>("log-verbosity");
    if (v < -1 || v > 5)
    {
        std::cerr << "verbosity only takes values between 0 and 5" << std::endl;
        THROW(error::config() << error::option("log-verbosity"));
    }
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

void configuration::conflicts(const char* opt1, const char* opt2) const
{
    if (check(opt1) && ! config_[opt1].defaulted()
            && check(opt2) && ! config_[opt2].defaulted())
    {
        std::cerr << "option --" << opt1 << " and --" << opt2
            << " are mutually exclusive" << std::endl;
        THROW(error::option_dependency());
    }
}

void configuration::depends(const char* for_what, const char* required) const
{
    if (check(for_what) && ! config_[for_what].defaulted() &&
            (! check(required) || config_[required].defaulted()))
    {
        std::cerr << "option --" << for_what << " depends on --"
            << required << std::endl;
        THROW(error::option_dependency());
    }
}

} // namespace util
} // namespace vast
