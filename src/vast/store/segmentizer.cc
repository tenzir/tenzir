#include <vast/store/segmentizer.h>

#include <ze/event.h>
#include <vast/util/logger.h>

namespace vast {
namespace store {

segmentizer::segmentizer(cppa::actor_ptr segment_manager,
                         size_t max_events_per_chunk,
                         size_t max_segment_size)
  : writer_(segment_)
  , segment_manager_(segment_manager)
{
  LOG(verbose, store)
    << "maximum segment size: " << max_segment_size << " bytes";
  LOG(verbose, store)
    << "maximum number of events per chunk: " << max_events_per_chunk;

  using namespace cppa;
  init_state = (
      on_arg_match >> [=](ze::event const& e)
      {
        auto n = writer_ << e;
        if (n < max_events_per_chunk)
          return;

        if (segment_.bytes() < max_segment_size)
        {
          writer_.flush_chunk();
          return;
        }

        send(segment_manager_, std::move(segment_));
        segment_ = segment();
      },
      on(atom("shutdown")) >> [=]()
      {
        if (writer_.bytes() > 0)
          send(segment_manager_, std::move(segment_));
          
        self->quit();
      });
}

} // namespace store
} // namespace vast
