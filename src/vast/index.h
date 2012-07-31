#ifndef VAST_INDEX_H
#define VAST_INDEX_H

#include <set>
#include <map>
#include <cppa/cppa.hpp>
#include <ze/uuid.h>
#include <ze/type/time.h>
#include "vast/segment.h"

namespace vast {

/// The event index.
class index : public cppa::sb_actor<index>
{
  friend class cppa::sb_actor<index>;

public:
  /// Spawns the index.
  /// @param directory The root directory of the index.
  index(cppa::actor_ptr archive, std::string directory);

private:
  /// Processes an incoming segment from the ingestor.
  void process(segment const& s);

  /// Writes a segment header to disk.
  void write(segment const& s);

  /// Build in-memory indices from a segment header.
  void build(segment::header const& hdr);

  std::string const dir_;
  cppa::actor_ptr archive_;
  cppa::behavior init_state;

  std::multimap<ze::time_point, ze::uuid> start_;
  std::multimap<ze::time_point, ze::uuid> end_;
  std::multimap<std::string, ze::uuid> event_names_;
  std::set<ze::uuid> ids_;
};

} // namespace vast

#endif
