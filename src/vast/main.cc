#include "vast/file_system.h"
#include "vast/logger.h"
#include "vast/program.h"
#include "vast/type_info.h"
#include "vast/detail/type_manager.h"

using namespace vast;

int main(int argc, char *argv[])
{
  auto config = configuration::parse(argc, argv);
  if (! config)
  {
    std::cerr << config.failure() << ", try -h or --help" << std::endl;
    return 1;
  }

  if (argc < 2 || config->check("help") || config->check("advanced"))
  {
    config->usage(std::cerr, config->check("advanced"));
    return 0;
  }

  auto vast_dir = path{*config->get("directory")};
  if (! exists(vast_dir) && ! mkdir(vast_dir))
  {
    std::cerr << "could not create directory: " << vast_dir << std::endl;
    return 1;
  }

  logger::instance()->init(
      *logger::parse_level(*config->get("log.console-verbosity")),
      *logger::parse_level(*config->get("log.file-verbosity")),
      config->check("log.function-names"),
      vast_dir / "log");

  VAST_LOG_VERBOSE(" _   _____   __________");
  VAST_LOG_VERBOSE("| | / / _ | / __/_  __/");
  VAST_LOG_VERBOSE("| |/ / __ |_\\ \\  / / ");
  VAST_LOG_VERBOSE("|___/_/ |_/___/ /_/  " << VAST_VERSION);
  VAST_LOG_VERBOSE("");

  announce_builtin_types();
  auto n = 0;
  detail::type_manager::instance()->each([&](global_type_info const&) { ++n; });
  VAST_LOG_DEBUG("type manager announced " << n << " types");

  // Hand control to libcppa.
  auto prog = cppa::spawn<program>(*config);
  cppa::await_all_others_done();
  cppa::shutdown();

  // Shutdown singletons.
  std::atomic_thread_fence(std::memory_order_seq_cst);
  detail::type_manager::destruct();
  logger::destruct();

  return 0;
}
