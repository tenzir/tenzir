#include "vast/store/segmentizer.h"

#include <ze/event.h>
#include "vast/util/logger.h"

namespace vast {
namespace store {

segmentizer::segmentizer(size_t max_events_per_chunk, size_t max_segment_size)
{
  using namespace cppa;

  LOG(verbose, store)
    << "maximum segment size: " << max_segment_size_ << " bytes";
  LOG(verbose, store)
    << "maximum number of events per chunk: " << max_events_per_chunk_;

  init_state = (
      on_arg_match >> [=](ze::event const& e)
      {
        auto n = segment_.put(event);
        if (n < max_events_per_chunk_)
          return;

        segment_.flush();
        if (segment_.size() < max_segment_size_)
        {
          segment_.push_chunk();
          return;
        }

        send(sink_, std::move(segment_));
        segment_ = osegment();
      },
      on(atom("shutdown")) >> [=]()
      {
        if (segment_->n_events() > 0u)
        {
          LOG(verbose, store) 
            << "flushing last segment with " 
            << segment_->n_events() << " events";

          segment_->flush();
          send(sink_, std::move(segment_));
        }
      });
}

} // namespace store
} // namespace vast
