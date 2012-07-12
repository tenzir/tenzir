#include "vast/store/segmentizer.h"

#include <ze/event.h>
#include "vast/util/logger.h"

namespace vast {
namespace store {

segmentizer::segmentizer(size_t max_events_per_chunk, size_t max_segment_size)
  : writer_(segment.write())
{
  LOG(verbose, store)
    << "maximum segment size: " << max_segment_size_ << " bytes";
  LOG(verbose, store)
    << "maximum number of events per chunk: " << max_events_per_chunk_;

  using namespace cppa;
  init_state = (
      on_arg_match >> [=](ze::event const& e)
      {
        auto n = writer_ << e;
        if (n < max_events_per_chunk_)
          return;

        if (segment_.size() < max_segment_size_)
        {
          writer_ = segment_.write();
          return;
        }

        send(sink_, std::move(segment_));
        segment().swap(segment_);
      },
      on(atom("shutdown")) >> [=]()
      {
        if (writer_->bytes() > 0)
          send(sink_, std::move(segment_));
          
        self->quit();
      });
}

} // namespace store
} // namespace vast
