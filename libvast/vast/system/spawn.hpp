#ifndef VAST_SYSTEM_SPAWN_HPP
#define VAST_SYSTEM_SPAWN_HPP

#include <string>

#include <caf/local_actor.hpp>

#include "vast/expected.hpp"

#include "vast/system/tracker.hpp"

namespace vast {
namespace system {

struct options {
  path dir;
  std::string label;
  caf::message params;
};

expected<caf::actor> spawn_archive(caf::local_actor* self, options opts);

expected<caf::actor> spawn_exporter(caf::local_actor* self, options opts);

expected<caf::actor> spawn_importer(caf::local_actor* self, options opts);

expected<caf::actor> spawn_index(caf::local_actor* self, options opts);

expected<caf::actor> spawn_metastore(caf::local_actor* self, options opts);

expected<caf::actor> spawn_profiler(caf::local_actor* self, options opts);

expected<caf::actor> spawn_source(caf::local_actor* self, options opts);

expected<caf::actor> spawn_sink(caf::local_actor* self, options opts);

} // namespace system
} // namespace vast

#endif
