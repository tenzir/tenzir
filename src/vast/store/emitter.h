#ifndef VAST_STORE_EMITTER_H
#define VAST_STORE_EMITTER_H

#include <cppa/cppa.hpp>
#include "vast/store/segment_manager.h"

namespace vast {
namespace store {

/// Reads events from archive's segment cache.
class emitter : public cppa::sb_actor<emitter>
{
  friend class cppa::sb_actor<emitter>;

public:
  /// Sets the initial behavior.
  emitter(segment_manager& sm, std::vector<ze::uuid> ids);

private:
  void emit_chunk();

  segment_manager& segment_manager_;
  std::vector<ze::uuid> ids_;
  std::vector<ze::uuid>::const_iterator current_;
  std::shared_ptr<isegment> segment_;

  cppa::actor_ptr sink_;
  cppa::behavior running_;
  cppa::behavior init_state;
};

} // namespace store
} // namespace vast

#endif
