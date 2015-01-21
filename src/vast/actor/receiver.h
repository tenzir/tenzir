#ifndef VAST_RECEIVER_H
#define VAST_RECEIVER_H

#include <set>
#include "vast/actor/actor.h"

namespace vast {

/// Receives chunks from IMPORTER, imbues them with an ID from TRACKER, and
/// relays them to ARCHIVE and INDEX.
class receiver : public flow_controlled_actor
{
public:
  /// Spawns a receiver.
  receiver();

  void at(caf::down_msg const& msg) override;
  caf::message_handler make_handler() override;
  std::string name() const override;

private:
  caf::actor identifier_;
  caf::actor archive_;
  caf::actor index_;
};

} // namespace vast

#endif
