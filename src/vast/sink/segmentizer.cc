#include "vast/sink/segmentizer.h"

#include "vast/event.h"
#include "vast/exception.h"
#include "vast/logger.h"

namespace vast {
namespace sink {

using namespace cppa;

segmentizer::segmentizer(actor_ptr upstream,
                         size_t max_events_per_chunk, size_t max_segment_size)
  : upstream_(upstream),
    max_events_per_chunk_(max_events_per_chunk),
    max_segment_size_(max_segment_size),
    stats_(std::chrono::seconds(1)),
    segment_(uuid::random()),
    writer_(&segment_)
{
}

void segmentizer::process(event const& e)
{
  writer_ << e;
  if (writer_.elements() % max_events_per_chunk_ != 0)
    return;

  writer_.flush();
  if (writer_.chunk_bytes() - writer_bytes_at_last_rotate_ < max_segment_size_)
    return;

  writer_bytes_at_last_rotate_ = writer_.chunk_bytes();

  auto n = segment_.events();
  send(upstream_, std::move(segment_));
  segment_ = segment(uuid::random());

  VAST_LOG_DEBUG("segmentizer @" << id() << " sent segment with " <<
                 n << " events to @" << upstream_->id());

  if (stats_.timed_add(1) && stats_.last() > 0)
  {
    send(upstream_, atom("statistics"), stats_.last());
    VAST_LOG_VERBOSE(
        "segmentizer @" << id() <<
        " ingests at rate " << stats_.last() << " events/sec" <<
        " (mean " << stats_.mean() <<
        ", median " << stats_.median() <<
        ", standard deviation " << std::sqrt(stats_.variance()) << ")");
  }
}

void segmentizer::before_exit()
{
  if (writer_.elements() > 0)
  {
    writer_.flush();
    auto n = segment_.events();
    send(upstream_, std::move(segment_));
    VAST_LOG_DEBUG("segmentizer @" << id() << " sent final segment with " <<
                   n << " events to @" << upstream_->id());
  }
}

} // namespace sink
} // namespace vast
