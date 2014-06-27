#include <cppa/cppa.hpp>
#include "vast.h"

int main(int argc, char *argv[])
{
  auto cfg = vast::configuration::parse(argc, argv);
  if (! cfg)
  {
    std::cerr << cfg.error() << ", try -h or --help" << std::endl;
    return 1;
  }

  if (argc < 2 || cfg->check("help") || cfg->check("advanced"))
  {
    cfg->usage(std::cerr, cfg->check("advanced"));
    return 0;
  }

  cppa::spawn<vast::program>(std::move(*cfg));
  cppa::await_all_actors_done();
  cppa::shutdown();

  vast::cleanup();

  return 0;
}
