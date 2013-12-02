#include "vast/program.h"

using namespace cppa;
using namespace vast;

int main(int argc, char *argv[])
{
  configuration config;
  try
  {
    config.load(argc, argv);
    if (argc < 2 || config.check("help") || config.check("advanced"))
    {
      config.usage(std::cerr, config.check("advanced"));
      return 0;
    }
  }
  catch (error::config const& e)
  {
    std::cerr << e.what() << ", try -h or --help" << std::endl;
    return 1;
  }

  if (! vast::initialize(config))
    return 1;

  auto prog = spawn<program, detached>(config);
  await_all_others_done();
  cppa::shutdown();
  vast::shutdown();

  return 0;
}
