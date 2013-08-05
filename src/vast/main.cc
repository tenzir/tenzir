#include "vast/program.h"

int main(int argc, char *argv[])
{
  vast::configuration config;
  try
  {
    config.load(argc, argv);
    if (argc < 2 || config.check("help") || config.check("advanced"))
    {
      config.usage(std::cerr, config.check("advanced"));
      return 0;
    }
  }
  catch (vast::error::config const& e)
  {
    std::cerr << e.what() << ", try -h or --help" << std::endl;
    return 1;
  }

  return vast::program(config).run() ? 0 : 1;
}
