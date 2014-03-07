#ifndef VAST_SEARCH_H
#define VAST_SEARCH_H

#include <cppa/cppa.hpp>
#include "vast/actor.h"
#include "vast/util/flat_set.h"

namespace vast {

struct search_actor : actor<search_actor>
{
  struct client_state
  {
    util::flat_set<cppa::actor_ptr> queries;
  };

  search_actor(cppa::actor_ptr archive,
               cppa::actor_ptr index,
               cppa::actor_ptr schema_manager);

  void act();
  char const* description() const;

  cppa::actor_ptr archive_;
  cppa::actor_ptr index_;
  cppa::actor_ptr schema_manager_;
  std::map<cppa::actor_ptr, client_state> clients_;
  std::string last_parse_error_;
};

} // namespace vast

#endif
