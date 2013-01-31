#ifndef VAST_SOURCE_ASYNCHRONOUS_H
#define VAST_SOURCE_ASYNCHRONOUS_H

#include <ze/event.h>
#include "vast/actor.h"

namespace vast {
namespace source {

/// An asynchronous source that provides buffers and relays events in batches.
/// Any child deriving from this class must be an actor.
class asynchronous
{
public:
  /// Constructs an asynchronous source.
  /// @param receiver The receiver of the event batches.
  /// @param batch_size The size of each event batch.
  asynchronous(actor_ptr receiver, size_t batch_size);

  /// Buffer an event and send the batch when having reached the limit.
  void buffer(ze::event event);

private:
  actor_ptr receiver_;
  size_t batch_size_ = 0;
  std::vector<ze::event> buffer_;
};

} // namespace source
} // namespace vast

#endif
