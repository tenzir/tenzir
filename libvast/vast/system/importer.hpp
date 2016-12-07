#ifndef VAST_SYSTEM_IMPORTER_HPP
#define VAST_SYSTEM_IMPORTER_HPP

#include <vector>

#include <caf/stateful_actor.hpp>

#include "vast/aliases.hpp"
#include "vast/event.hpp"

#include "vast/system/archive.hpp"

namespace vast {
namespace system {

/// Receives chunks from SOURCEs, imbues them with an ID, and relays them to
/// ARCHIVE and INDEX.
struct importer_state {
  caf::actor identifier;
  archive_type archive;
  caf::actor index;
  event_id got = 0;
  std::vector<event> batch;
  const char* name = "importer";
};

/// Spawns an IMPORTER.
/// @param self The actor handle.
caf::behavior importer(caf::stateful_actor<importer_state>* self);

} // namespace system
} // namespace vast

#endif
