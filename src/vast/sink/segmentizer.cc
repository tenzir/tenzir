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
    stats_(std::chrono::seconds(1)),
    segment_(uuid::random(), max_segment_size),
    writer_(&segment_, max_events_per_chunk)
{
}

void segmentizer::process(event const& e)
{
  if (writer_.write(e))
  {
    if (stats_.timed_add(1) && stats_.last() > 0)
    {
      send(upstream_, atom("statistics"), stats_.last());
      VAST_LOG_ACT_VERBOSE(
          "segmentizer",
          "ingests at rate " << stats_.last() << " events/sec" <<
          " (mean " << stats_.mean() <<
          ", median " << stats_.median() <<
          ", standard deviation " << std::sqrt(stats_.variance()) << ")");
    }
    return;
  }

  VAST_LOG_ACT_DEBUG("segmentizer", "sends segment " << segment_.id() <<
                     " with " << segment_.events() << " events to @" <<
                     upstream_->id());

  auto max_segment_size = segment_.max_size();
  send(upstream_, std::move(segment_));
  segment_ = segment(uuid::random(), max_segment_size);
  writer_.attach_to(&segment_);
}

void segmentizer::before_exit()
{
  if (! writer_.flush())
  {
    segment_ = segment(uuid::random());
    writer_.attach_to(&segment_);
    if (! writer_.flush())
      VAST_LOG_ACT_ERROR("segmentizer", "failed to flush a fresh segment");
    assert(segment_.events() > 0);
  }

  VAST_LOG_ACT_DEBUG("segmentizer", "sends final segment " << segment_.id() <<
                     " with " << segment_.events() << " events to @" <<
                     upstream_->id());

  send(upstream_, std::move(segment_));
}

} // namespace sink
} // namespace vast
