#include "vast/system/spawn_disk_monitor.hpp"

#include "vast/concept/parseable/vast/si.hpp"
#include "vast/defaults.hpp"
#include "vast/logger.hpp"
#include "vast/path.hpp"
#include "vast/system/disk_monitor.hpp"
#include "vast/system/node.hpp"
#include "vast/system/node_control.hpp"
#include "vast/system/spawn_arguments.hpp"

#include <caf/actor.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/expected.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/settings.hpp>
#include <caf/typed_event_based_actor.hpp>

#include "caf/error.hpp"

namespace vast::system {

maybe_actor
spawn_disk_monitor(system::node_actor* self, spawn_arguments& args) {
  VAST_TRACE(VAST_ARG(args));
  auto [index, archive]
    = self->state.registry.find_by_label("index", "archive");
  if (!index)
    return make_error(ec::missing_component, "index");
  if (!archive)
    return make_error(ec::missing_component, "archive");
  auto hiwater_str
    = caf::get_or(args.inv.options, "vast.start.disk-budget-high", "0KiB");
  auto lowater_str
    = caf::get_or(args.inv.options, "vast.start.disk-budget-low", "0KiB");
  // TODO: This
  auto default_seconds
    = std::chrono::seconds{defaults::system::disk_scan_interval}.count();
  auto interval = caf::get_or(
    args.inv.options, "vast.start.disk-budget-check-interval", default_seconds);
  uint64_t hiwater, lowater;
  if (!parsers::bytesize(hiwater_str, hiwater))
    return make_error(ec::parse_error,
                      "could not parse disk budget: " + hiwater_str);
  if (!parsers::bytesize(lowater_str, lowater))
    return make_error(ec::parse_error,
                      "could not parse disk budget: " + lowater_str);
  if (!hiwater) {
    VAST_VERBOSE(self, "not spawning disk_monitor because no limit configured");
    return ec::no_error;
  }
  // Set low == high as the default value.
  if (!lowater)
    lowater = hiwater;
  auto db_dir = caf::get_or(args.inv.options, "vast.db-directory",
                            defaults::system::db_directory);
  auto abs_dir = path{db_dir}.complete();
  if (!exists(abs_dir))
    return make_error(ec::filesystem_error, "could not find database "
                                            "directory");
  auto handle = self->spawn(disk_monitor, hiwater, lowater,
                            std::chrono::seconds{interval}, abs_dir,
                            caf::actor_cast<archive_actor>(archive),
                            caf::actor_cast<index_actor>(index));
  VAST_VERBOSE(self, "spawned a disk monitor");
  return caf::actor_cast<caf::actor>(handle);
}

} // namespace vast::system
