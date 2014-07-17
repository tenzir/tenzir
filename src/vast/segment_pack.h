#ifndef VAST_SEGMENT_PACK_H
#define VAST_SEGMENT_PACK_H

#include "vast/actor.h"
#include "vast/segment.h"

namespace vast {

/// Packs events into segments.
struct packer : actor_base
{
  packer(caf::actor manager, caf::actor sink);

  caf::message_handler act() final;
  std::string describe() const final;

  caf::actor manager_;
  caf::actor sink_;
  segment segment_;
  segment::writer writer_;
};

/// Unpacks a segment into batches of events.
struct unpacker : actor_base
{
  unpacker(caf::message segment, caf::actor sink, size_t batch_size = 0);

  caf::message_handler act() final;
  std::string describe() const final;

  caf::message segment_;
  segment::reader reader_;
  caf::actor sink_;
  std::vector<event> events_;
  size_t batch_size_;
};

} // namespace vast

#endif
