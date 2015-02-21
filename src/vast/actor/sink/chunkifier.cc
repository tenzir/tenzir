#include "vast/actor/sink/chunkifier.h"

#include <caf/all.hpp>
#include "vast/event.h"
#include "vast/logger.h"

namespace vast {
namespace sink {

using namespace caf;

chunkifier::chunkifier(actor const& upstream, size_t max_events_per_chunk,
                       io::compression method)
  : base<chunkifier>{"chunkifier"},
    upstream_{upstream},
    compression_{method},
    chunk_{std::make_unique<chunk>(compression_)},
    writer_{std::make_unique<chunk::writer>(*chunk_)},
    max_events_per_chunk_{max_events_per_chunk}
{
  attach_functor([=](uint32_t) { upstream_ = invalid_actor; });
}

void chunkifier::finalize()
{
  writer_->flush();
  if (chunk_->events() > 0)
    send(upstream_, std::move(*chunk_));
  if (total_events_ > 0)
    VAST_VERBOSE(this, "processed", total_events_, "events");
}

bool chunkifier::process(event const& e)
{
  if (! writer_->write(e))
  {
    VAST_ERROR(this, "failed to write event into chunk:", e);
    quit(exit::error);
    return false;
  }
  ++total_events_;
  if (chunk_->events() == max_events_per_chunk_)
  {
    writer_.reset();
    send(upstream_, std::move(*chunk_));
    chunk_ = std::make_unique<chunk>();
    writer_ = std::make_unique<chunk::writer>(*chunk_);
  }
  return true;
}

} // namespace sink
} // namespace vast
