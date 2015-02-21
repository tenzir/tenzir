#ifndef VAST_ACTOR_SEARCH_H
#define VAST_ACTOR_SEARCH_H

#include "vast/actor/actor.h"
#include "vast/util/flat_set.h"

namespace vast {

class expression;

struct search : default_actor
{
  search();
  void on_exit();
  caf::behavior make_behavior() override;

  caf::actor archive_;
  caf::actor index_;
  std::multimap<caf::actor_addr, caf::actor> queries_;
};

} // namespace vast

#endif
