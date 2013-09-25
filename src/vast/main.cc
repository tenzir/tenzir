#include "vast/program.h"
#include "vast/shutdown.h"

using namespace cppa;
using namespace vast;

bool run(configuration const& config)
{
  auto prog = spawn<program, detached>(config);
  self->monitor(prog);
  bool error = false;
  receive(
      on(atom("DOWN"), exit_reason::user_defined) >> [&]
      {
        error = true;
      });

  return ! error;
}

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

  auto exit_code = run(config) ? 0 : 1;
  await_all_others_done();
  cppa::shutdown();
  vast::shutdown();

  return exit_code;
}
