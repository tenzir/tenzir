#include "framework/unit.h"
#include "vast.h"
#include "vast/file_system.h"
#include "vast/logger.h"
#include "vast/type_info.h"

int main(int argc, char* argv[])
{
  vast::announce_builtin_types();

  auto cfg = unit::configuration::parse(argc, argv);
  if (! cfg)
  {
    std::cerr << cfg.error() << '\n';
    return 1;
  }

  auto log_dir = vast::path{*cfg->get("vast-log-dir")};

  if (! vast::logger::instance()->init(
          vast::logger::quiet,
          vast::logger::debug,
          false, false, log_dir))
  {
    std::cerr << "failed to initialize VAST's logger" << std::endl;
    return 1;
  }

  auto rc = unit::engine::run(*cfg);

  if (! cfg->check("vast-keep-logs"))
    vast::rm(log_dir);

  return vast::cleanup() && rc;
}
