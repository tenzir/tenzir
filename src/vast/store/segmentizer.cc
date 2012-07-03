#include "vast/store/segmentizer.h"

#include <ze/event.h>
#include "vast/fs/fstream.h"
#include "vast/fs/operations.h"
#include "vast/store/segment.h"
#include "vast/util/logger.h"

namespace vast {
namespace store {

segmentizer::segmentizer(ze::component& c)
  : device(c)
{
    frontend().receive(
        [&](ze::event_ptr event)
        {
            std::lock_guard<std::mutex> lock(segment_mutex_);
            if (! segment_)
            {
                LOG(debug, store)
                    << "segmentizer couldn't accommodate event: " << *event;
                return;
            }

            auto events = segment_->put(*event);
            if (events < max_events_per_chunk_)
                return;

            segment_->flush();
            if (segment_->size() < max_segment_size_)
            {
                segment_->push_chunk();
                return;
            }

            backend().send(segment_);
            segment_ = new osegment;
        });
}

segmentizer::~segmentizer()
{
    if (segment_ && ! terminating_)
        stop();
}

void segmentizer::init(size_t max_events_per_chunk, size_t max_segment_size)
{
    max_events_per_chunk_ = max_events_per_chunk;
    max_segment_size_ = max_segment_size;

    LOG(verbose, store)
        << "maximum segment size: " << max_segment_size_ << " bytes";
    LOG(verbose, store)
        << "maximum number of events per chunk: " << max_events_per_chunk_;

    segment_ = new osegment;
}

void segmentizer::stop()
{
    terminating_ = true;
    std::lock_guard<std::mutex> lock(segment_mutex_);
    if (segment_->n_events() > 0u)
    {
        segment_->flush();
        backend().send(segment_);
    }
    segment_.reset();
}

} // namespace store
} // namespace vast
