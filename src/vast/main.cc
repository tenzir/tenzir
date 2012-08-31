#include <cstdlib>
#include "vast/program.h"

int main(int argc, char *argv[])
{
  vast::configuration config;
  if (! config.load(argc, argv))
    return EXIT_FAILURE;

  if (argc < 2 || config.check("help") || config.check("advanced"))
  {
    config.print(std::cerr, config.check("advanced"));
    return EXIT_SUCCESS;
  }

  vast::program program(config);
  auto rc = program.run();

  return rc ? EXIT_SUCCESS : EXIT_FAILURE;
}
