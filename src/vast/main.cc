#include <cstdlib>
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
      return EXIT_SUCCESS;
    }
    
    vast::init(config);
    return vast::program(config).run() ? EXIT_SUCCESS : EXIT_FAILURE;
  }
  catch (vast::error::config const& e)
  {
    std::cerr << e.what() << ", try -h or --help" << std::endl;
    return EXIT_FAILURE;
  }
}
