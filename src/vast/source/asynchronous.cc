#include "vast/source/asynchronous.h"

namespace vast {
namespace source {

using namespace cppa;

asynchronous::asynchronous(actor_ptr receiver, size_t batch_size)
  : receiver_(receiver),
    batch_size_(batch_size)
{
  assert(batch_size_ > 0);
}

void asynchronous::buffer(ze::event event)
{
  buffer_.push_back(std::move(event)); 
  if (buffer_.size() < batch_size_)
    return;
  send(receiver_, std::move(events_));
  events_.clear();
}

} // namespace source
} // namespace vast
