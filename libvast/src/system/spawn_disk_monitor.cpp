#include "vast/system/spawn_disk_monitor.hpp"

#include "vast/concept/parseable/vast/si.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/settings.hpp"
#include "vast/logger.hpp"
#include "vast/path.hpp"
#include "vast/system/disk_monitor.hpp"
#include "vast/system/node.hpp"
#include "vast/system/node_control.hpp"
#include "vast/system/spawn_arguments.hpp"
#include "vast/uuid.hpp"

#include <caf/settings.hpp>
#include <caf/typed_event_based_actor.hpp>

namespace vast::system {

caf::expected<caf::actor>
spawn_disk_monitor(node_actor::stateful_pointer<node_state> self,
                   spawn_arguments& args) {
  VAST_TRACE_SCOPE("{}", VAST_ARG(args));
  auto [index, archive]
    = self->state.registry.find<index_actor, archive_actor>();
  if (!index)
    return caf::make_error(ec::missing_component, "index");
  if (!archive)
    return caf::make_error(ec::missing_component, "archive");
  auto opts = args.inv.options;
  auto hiwater = detail::get_bytesize(opts, "vast.start.disk-budget-high", 0);
  auto lowater = detail::get_bytesize(opts, "vast.start.disk-budget-low", 0);
  if (!hiwater)
    return hiwater.error();
  if (!lowater)
    return lowater.error();
  if (!*hiwater) {
    VAST_VERBOSE("{} not spawning disk_monitor because no limit configured",
                 self);
    return ec::no_error;
  }
  // Set low == high as the default value.
  if (!*lowater)
    *lowater = *hiwater;
  auto default_seconds
    = std::chrono::seconds{defaults::system::disk_scan_interval}.count();
  auto interval = caf::get_or(opts, "vast.start.disk-budget-check-interval",
                              default_seconds);
  const auto db_dir
    = caf::get_or(opts, "vast.db-directory", defaults::system::db_directory);
  const auto db_dir_path = std::filesystem::path{db_dir};
  std::error_code ec{};
  const auto db_dir_abs = std::filesystem::absolute(db_dir_path, ec);
  if (ec)
    return caf::make_error(ec::filesystem_error, "could not make absolute path "
                                                 "to database directory");
  if (!std::filesystem::exists(db_dir_abs))
    return caf::make_error(ec::filesystem_error, "could not find database "
                                                 "directory");
  auto handle
    = self->spawn(disk_monitor, *hiwater, *lowater,
                  std::chrono::seconds{interval}, db_dir_abs, archive, index);
  VAST_VERBOSE("{} spawned a disk monitor", self);
  return caf::actor_cast<caf::actor>(handle);
}

} // namespace vast::system
