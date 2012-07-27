#ifndef VAST_EMITTER_H
#define VAST_EMITTER_H

// TODO: Currently, no more available segment IDs means that we're done.
// Eventually, emitters should request more IDs from the index once they have
// only a few left (i.e., reached a minimum-number-of-IDs-threshold).

#include <deque>
#include <cppa/cppa.hpp>
#include <ze/uuid.h>

namespace vast {

// Forward declarations.
class segment;

/// Reads events from archive's segment cache.
class emitter : public cppa::sb_actor<emitter>
{
  friend class cppa::sb_actor<emitter>;

public:
  /// Spawns an emitter.
  /// @param segment_manager The segment manager to ask for segments.
  /// @param sink The actor receiving chunks.
  emitter(cppa::actor_ptr segment_manager, cppa::actor_ptr sink);

private:
  void retrieve_segment();
  void emit_chunk();

  std::deque<ze::uuid> ids_;
  cppa::cow_tuple<segment> segment_tuple_;
  size_t current_chunk_ = 0;
  size_t last_chunk_ = 0;
  segment const* segment_ = nullptr;

  cppa::actor_ptr segment_manager_;
  cppa::actor_ptr sink_;
  cppa::behavior running_;
  cppa::behavior init_state;
};

} // namespace vast

#endif
