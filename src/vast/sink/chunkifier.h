#ifndef VAST_SINK_CHUNKIFIER_H
#define VAST_SINK_CHUNKIFIER_H

#include "vast/chunk.h"
#include "vast/util/accumulator.h"
#include "vast/sink/base.h"

namespace vast {
namespace sink {

/// Receives events from sources, writes them into chunks, and then relays the
/// chunks them upstream.
class chunkifier : public base<chunkifier>
{
public:
  /// Spawns a chunkifier.
  /// @param upstream The upstream actor receiving the generated chunks.
  /// @param max_events_per_chunk The maximum number of events per chunk.
  chunkifier(caf::actor upstream, size_t max_events_per_chunk);

  bool process(event const& e);
  std::string describe() const final;

private:
  caf::actor upstream_;
  std::unique_ptr<chunk> chunk_;
  std::unique_ptr<chunk::writer> writer_;
  util::rate_accumulator<uint64_t> stats_;
  size_t max_events_per_chunk_;
  size_t total_events_ = 0;
};

} // namespace sink
} // namespace vast

#endif
