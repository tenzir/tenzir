#ifndef VAST_RECEIVER_H
#define VAST_RECEIVER_H

#include <set>
#include "vast/actor/actor.h"

namespace vast {

/// Receives chunks from IMPORTER, imbues them with an ID from TRACKER, and
/// relays them to ARCHIVE and INDEX.
class receiver : public actor_mixin<receiver, flow_controlled, sentinel>
{
public:
  /// Spawns a receiver.
  receiver();

  caf::message_handler make_handler();
  void at_exit(caf::exit_msg const&);
  std::string name() const;

private:
  caf::actor identifier_;
  caf::actor archive_;
  caf::actor index_;
};

} // namespace vast

#endif
