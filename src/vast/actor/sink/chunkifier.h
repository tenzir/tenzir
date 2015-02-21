#ifndef VAST_SINK_CHUNKIFIER_H
#define VAST_SINK_CHUNKIFIER_H

#include "vast/chunk.h"
#include "vast/util/accumulator.h"
#include "vast/actor/sink/base.h"

namespace vast {
namespace sink {

/// Receives events from sources, writes them into chunks, and then relays the
/// chunks them upstream.
struct chunkifier : base<chunkifier>
{
  /// Spawns a chunkifier.
  /// @param upstream The upstream actor receiving the generated chunks.
  /// @param max_events_per_chunk The maximum number of events per chunk.
  /// @param method The compression method to use for the chunksl
  chunkifier(caf::actor const& upstream,
             size_t max_events_per_chunk,
             io::compression method);

  void finalize() override;
  bool process(event const& e);

  caf::actor upstream_;
  io::compression compression_;
  std::unique_ptr<chunk> chunk_;
  std::unique_ptr<chunk::writer> writer_;
  size_t max_events_per_chunk_;
  size_t total_events_ = 0;
};

} // namespace sink
} // namespace vast

#endif
