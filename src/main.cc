#include <caf/all.hpp>
#include "vast.h"

int main(int argc, char *argv[])
{
  auto cfg = vast::configuration::parse(argc, argv);
  if (! cfg)
  {
    std::cerr << cfg.error() << ", try -h or -z" << std::endl;
    return 1;
  }

  if (argc < 2 || cfg->check("help") || cfg->check("advanced"))
  {
    cfg->usage(std::cerr, cfg->check("advanced"));
    return 0;
  }
  else if (cfg->check("version"))
  {
    std::cout << VAST_VERSION << std::endl;
    return 0;
  }

  vast::announce_builtin_types();

  auto dir = vast::path{*cfg->get("directory")}.complete();
  auto initialized = vast::logger::instance()->init(
      *vast::logger::parse_level(*cfg->get("log.console")),
      *vast::logger::parse_level(*cfg->get("log.file")),
      ! cfg->check("log.no-colors"),
      cfg->check("log.function-names"),
      dir / *cfg->get("log.directory"));

  if (! initialized)
  {
    std::cerr << "failed to initialize logger" << std::endl;
    return 1;
  }

  VAST_VERBOSE(" _   _____   __________");
  VAST_VERBOSE("| | / / _ | / __/_  __/");
  VAST_VERBOSE("| |/ / __ |_\\ \\  / / ");
  VAST_VERBOSE("|___/_/ |_/___/ /_/  " << VAST_VERSION);
  VAST_VERBOSE("");

  auto threads = std::thread::hardware_concurrency();
  if (auto t = cfg->as<size_t>("caf.threads"))
    threads = *t;

  auto throughput = std::numeric_limits<size_t>::max();
  if (auto t = cfg->as<size_t>("caf.throughput"))
    throughput = *t;

  caf::set_scheduler<>(threads, throughput);
  VAST_VERBOSE("set scheduler threads to", threads);
  VAST_VERBOSE("set scheduler maximum throughput to",
               (throughput == std::numeric_limits<size_t>::max()
                ? "unlimited" : std::to_string(throughput)));

  auto program = caf::spawn<vast::program>(std::move(*cfg));
  caf::scoped_actor self;
  self->sync_send(program, vast::run_atom::value).await(
    [](caf::ok_atom) {},
    [&](vast::error const& e)
    {
      VAST_ERROR(program, "encountered error:", e);
    },
    caf::others() >> [&]
    {
      VAST_WARN(program, "got unexpected message:",
                to_string(self->last_dequeued()));
    });
  self->await_all_other_actors_done();
  caf::shutdown();
  vast::cleanup();

  auto er = program->exit_reason();
  if (er == vast::exit::done || er == vast::exit::stop)
    return 0;
  else if (er == vast::exit::error)
    return 1;
  else if (er == vast::exit::kill)
    return 2;
  else
    return 255;
}
