#include "vast/system/spawn_disk_monitor.hpp"

#include "vast/concept/parseable/vast/si.hpp"
#include "vast/defaults.hpp"
#include "vast/logger.hpp"
#include "vast/path.hpp"
#include "vast/system/disk_monitor.hpp"
#include "vast/system/node.hpp"
#include "vast/system/node_control.hpp"
#include "vast/system/spawn_arguments.hpp"
#include "vast/uuid.hpp"

#include <caf/settings.hpp>

namespace vast::system {

maybe_actor
spawn_disk_monitor(system::node_actor* self, spawn_arguments& args) {
  VAST_TRACE("{}", detail::id_or_name(VAST_ARG(args)));
  auto [index, archive]
    = self->state.registry.find<index_actor, archive_actor>();
  if (!index)
    return caf::make_error(ec::missing_component, "index");
  if (!archive)
    return caf::make_error(ec::missing_component, "archive");
  auto opts = args.inv.options;
  caf::put_missing(opts, "vast.start.disk-budget-high", "0KiB");
  caf::put_missing(opts, "vast.start.disk-budget-low", "0KiB");
  // TODO: Also allow integer arguments. Currently we insist on a string because
  // `caf::get_or` below will silently take the default value of "0KiB" if the
  // key is not a string. (e.g. `disk-budget-high: 6T`)
  if (!caf::holds_alternative<std::string>(opts, "vast.start.disk-budget-high"))
    return caf::make_error(ec::invalid_argument,
                           "could not parse disk-budget-high "
                           "as byte size");
  if (!caf::holds_alternative<std::string>(opts, "vast.start.disk-budget-low"))
    return caf::make_error(ec::invalid_argument,
                           "could not parse disk-budget-low "
                           "as byte size");
  auto hiwater_str = caf::get<std::string>(opts, "vast.start.disk-budget-high");
  auto lowater_str = caf::get<std::string>(opts, "vast.start.disk-budget-low");
  auto default_seconds
    = std::chrono::seconds{defaults::system::disk_scan_interval}.count();
  auto interval = caf::get_or(opts, "vast.start.disk-budget-check-interval",
                              default_seconds);
  uint64_t hiwater, lowater;
  if (!parsers::bytesize(hiwater_str, hiwater))
    return caf::make_error(ec::parse_error,
                           "could not parse disk budget: " + hiwater_str);
  if (!parsers::bytesize(lowater_str, lowater))
    return caf::make_error(ec::parse_error,
                           "could not parse disk budget: " + lowater_str);
  if (!hiwater) {
    VAST_VERBOSE("{} not spawning disk_monitor because no limit "
                 "configured",
                 detail::id_or_name(self));
    return ec::no_error;
  }
  // Set low == high as the default value.
  if (!lowater)
    lowater = hiwater;
  auto db_dir
    = caf::get_or(opts, "vast.db-directory", defaults::system::db_directory);
  auto abs_dir = path{db_dir}.complete();
  if (!exists(abs_dir))
    return caf::make_error(ec::filesystem_error, "could not find database "
                                                 "directory");
  auto handle
    = self->spawn(disk_monitor, hiwater, lowater,
                  std::chrono::seconds{interval}, abs_dir, archive, index);
  VAST_VERBOSE("{} spawned a disk monitor", detail::id_or_name(self));
  return caf::actor_cast<caf::actor>(handle);
}

} // namespace vast::system
