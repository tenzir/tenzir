#ifndef VAST_ACTOR_SEARCH_H
#define VAST_ACTOR_SEARCH_H

#include "vast/actor/actor.h"
#include "vast/util/flat_set.h"

namespace vast {

struct search : public actor_mixin<search, sentinel>
{
  struct client_state
  {
    util::flat_set<caf::actor> queries;
  };

  search();

  caf::message_handler make_handler();
  std::string name() const;

  caf::actor archive_;
  caf::actor index_;
  std::map<caf::actor_addr, client_state> clients_;
};

} // namespace vast

#endif
