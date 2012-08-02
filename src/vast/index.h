#ifndef VAST_INDEX_H
#define VAST_INDEX_H

#include <set>
#include <map>
#include <cppa/cppa.hpp>
#include <ze/uuid.h>
#include <ze/type/string.h>
#include "vast/segment.h"

namespace vast {

/// The event index.
class index : public cppa::sb_actor<index>
{
  friend class cppa::sb_actor<index>;

public:
  /// The in-memory index for event meta data;
  struct meta
  {
    typedef std::pair<ze::time_point, ze::time_point> interval;
    std::multimap<interval, ze::uuid> ranges;
    std::multimap<ze::string, ze::uuid> names;
  };

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

  std::set<ze::uuid> ids_;
  meta meta_;
};

} // namespace vast

#endif
