#ifndef VAST_SEGMENT_PACK_H
#define VAST_SEGMENT_PACK_H

#include "vast/actor.h"
#include "vast/segment.h"

namespace vast {

/// Packs events into segments.
struct packer : actor_base
{
  packer(cppa::actor manager, cppa::actor sink);

  cppa::partial_function act() final;
  std::string describe() const final;

  cppa::actor manager_;
  cppa::actor sink_;
  segment segment_;
  segment::writer writer_;
};

/// Unpacks a segment into batches of events.
struct unpacker : actor_base
{
  unpacker(cppa::any_tuple segment, cppa::actor sink, size_t batch_size = 0);

  cppa::partial_function act() final;
  std::string describe() const final;

  cppa::any_tuple segment_;
  segment::reader reader_;
  cppa::actor sink_;
  std::vector<event> events_;
  size_t batch_size_;
};

} // namespace vast

#endif
