#include "vast/sink/chunkifier.h"

#include <caf/all.hpp>
#include "vast/event.h"
#include "vast/logger.h"

namespace vast {
namespace sink {

using namespace caf;

chunkifier::chunkifier(actor upstream, size_t max_events_per_chunk)
  : upstream_{upstream},
    chunk_{std::make_unique<chunk>()},
    writer_{std::make_unique<chunk::writer>(*chunk_)},
    stats_{std::chrono::seconds(1)},
    max_events_per_chunk_{max_events_per_chunk}
{
}

bool chunkifier::process(event const& e)
{
  if (! writer_->write(e))
  {
    VAST_LOG_ACTOR_ERROR("failed to write event into chunk: " << e);
    quit(exit::error);
    return false;
  }

  ++total_events_;
  if (stats_.increment())
    VAST_LOG_ACTOR_VERBOSE(
        "writes at " << stats_.last() << " events/sec (" <<
        stats_.mean() << '/' << stats_.median() << '/' <<
        stats_.sd() << " mean/median/sd)");


  if (chunk_->events() == max_events_per_chunk_)
  {
    writer_.reset();
    send(upstream_, std::move(*chunk_));
    chunk_ = std::make_unique<chunk>();
    writer_ = std::make_unique<chunk::writer>(*chunk_);
  }

  return true;
}

void chunkifier::finalize()
{
  writer_->flush();
  if (chunk_->events() > 0)
    send(upstream_, std::move(*chunk_));

  upstream_ = invalid_actor;

  if (total_events_ > 0)
    VAST_LOG_ACTOR_VERBOSE("processed " << total_events_ << " events");
}

std::string chunkifier::describe() const
{
  return "chunkifier";
}

} // namespace sink
} // namespace vast
