#ifndef VAST_SEARCH_H
#define VAST_SEARCH_H

#include <cppa/cppa.hpp>
#include "vast/actor.h"
#include "vast/file_system.h"
#include "vast/schema.h"
#include "vast/util/flat_set.h"

namespace vast {

struct search_actor : actor<search_actor>
{
  struct client_state
  {
    util::flat_set<cppa::actor_ptr> queries;
    uint64_t batch_size = 1; // Overridden by client.
  };

  search_actor(path dir, cppa::actor_ptr archive, cppa::actor_ptr index);

  void act();
  char const* description() const;

  path dir_;
  schema schema_;
  cppa::actor_ptr archive_;
  cppa::actor_ptr index_;
  std::map<cppa::actor_ptr, client_state> clients_;
  error last_parse_error_;
};

} // namespace vast

#endif
