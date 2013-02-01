#ifndef VAST_SOURCE_SYNCHRONOUS_H
#define VAST_SOURCE_SYNCHRONOUS_H

#include "vast/actor.h"

namespace vast {
namespace source {

/// A synchronous source that extracts events one by one.
class synchronous : public minion<synchronous>
{
  friend class minion<synchronous>;

public:
  synchronous(cppa::actor_ptr upstream, size_t batch_size);

  virtual ze::event extract() = 0;

protected:
  bool finished_ = false;

private:
  size_t errors_ = 0;
  std::vector<ze::event> events_;
  cppa::behavior operating_;
};

} // namespace source
} // namespace vast

#endif
