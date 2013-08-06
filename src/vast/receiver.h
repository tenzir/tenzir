#ifndef VAST_RECEIVER_H
#define VAST_RECEIVER_H

#include <cppa/cppa.hpp>

namespace vast {

/// Receives segments and relays them to archive and index.
class receiver : public cppa::event_based_actor
{
public:
  /// Spawns a receiver.
  receiver();

  /// Implements `event_based_actor::run`.
  virtual void init() final;

private:
};

} // namespace vast

#endif
