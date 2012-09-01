#include <cstdlib>
#include "vast/program.h"

int main(int argc, char *argv[])
{
  vast::configuration config;
  if (argc < 2 || ! config.load(argc, argv))
  {
    config.print(std::cerr, config.check("advanced"));
    return EXIT_FAILURE;
  }

  vast::program program(config);
  auto rc = program.run();

  return rc ? EXIT_SUCCESS : EXIT_FAILURE;
}
