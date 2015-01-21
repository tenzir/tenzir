#ifndef VAST_ACTOR_SEARCH_H
#define VAST_ACTOR_SEARCH_H

#include "vast/actor/actor.h"
#include "vast/util/flat_set.h"

namespace vast {

struct search : public default_actor
{
  struct client_state
  {
    util::flat_set<caf::actor> queries;
  };

  search();

  void at(caf::exit_msg const& msg) override;
  void at(caf::down_msg const& msg) override;
  caf::message_handler make_handler() override;
  std::string name() const override;

  caf::actor archive_;
  caf::actor index_;
  std::map<caf::actor_addr, client_state> clients_;
};

} // namespace vast

#endif
