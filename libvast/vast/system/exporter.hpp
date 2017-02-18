#ifndef VAST_SYSTEM_EXPORTER_HPP
#define VAST_SYSTEM_EXPORTER_HPP

#include <chrono>
#include <deque>
#include <memory>
#include <unordered_map>

#include "vast/aliases.hpp"
#include "vast/bitmap.hpp"
#include "vast/expression.hpp"
#include "vast/query_options.hpp"
#include "vast/uuid.hpp"

#include "vast/system/accountant.hpp"
#include "vast/system/archive.hpp"
#include "vast/system/query_statistics.hpp"

namespace vast {
namespace system {

struct exporter_state {
  archive_type archive;
  caf::actor index;
  caf::actor sink;
  accountant_type accountant;
  bitmap hits;
  bitmap unprocessed;
  std::unordered_map<type, expression> checkers;
  std::deque<event> candidates;
  std::vector<event> results;
  std::chrono::steady_clock::time_point start;
  query_statistics stats;
  uuid id;
  char const* name = "exporter";
};

/// The EXPORTER receives index hits, looks up the corresponding events in the
/// archive, and performs a candidate check to select the resulting stream of
/// matching events.
/// @param self The actor handle.
/// @param ast The AST of query.
/// @param qos The query options.
caf::behavior exporter(caf::stateful_actor<exporter_state>* self,
                       expression expr, query_options opts);

} // namespace system
} // namespace vast

#endif
