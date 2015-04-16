#include <caf/set_scheduler.hpp>

#include "vast/announce.h"
#include "vast/filesystem.h"
#include "vast/logger.h"

#include "framework/unit.h"

int main(int argc, char* argv[])
{
  vast::announce_types();
  // Because we use multiple non-detached blocking actors in the unit tests, we
  // need to have enough workers available to avoid a deadlock.
  if (std::thread::hardware_concurrency() < 4)
    caf::set_scheduler<>(4);
  auto cfg = unit::configuration::parse(argc, argv);
  if (! cfg)
  {
    std::cerr << cfg.error() << '\n';
    return 1;
  }
  auto log_file = vast::path{*cfg->get("vast-log-dir")} / "vast.log";
  if (! vast::logger::file(vast::logger::debug, log_file.str()))
  {
    std::cerr << "failed to initialize VAST's logger" << std::endl;
    return 1;
  }
  auto rc = unit::engine::run(*cfg);
  if (! cfg->check("vast-keep-logs"))
    vast::rm(log_file);
  vast::logger::destruct();
  return rc ? 0 : 1;
}
