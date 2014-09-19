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

  auto threads = std::thread::hardware_concurrency();
  if (auto t = cfg->as<size_t>("caf.threads"))
    threads = *t;

  auto throughput = std::numeric_limits<size_t>::max();
  if (auto t = cfg->as<size_t>("caf.throughput"))
    throughput = *t;

  caf::set_scheduler<>(threads, throughput);

  auto program = caf::spawn<vast::program>(std::move(*cfg));
  caf::anon_send(program, caf::atom("run"));

  caf::await_all_actors_done();
  caf::shutdown();

  vast::cleanup();

  return 0;
}
