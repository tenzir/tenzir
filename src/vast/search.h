#ifndef VAST_SEARCH_H
#define VAST_SEARCH_H

#include <cppa/cppa.hpp>
#include "vast/actor.h"
#include "vast/file_system.h"
#include "vast/schema.h"
#include "vast/util/flat_set.h"

namespace vast {

struct search_actor : actor_base
{
  struct client_state
  {
    util::flat_set<cppa::actor> queries;
  };

  search_actor(path dir, cppa::actor archive, cppa::actor index);

  cppa::partial_function act() final;
  std::string describe() const final;

  path dir_;
  schema schema_;
  cppa::actor archive_;
  cppa::actor index_;
  std::map<cppa::actor_addr, client_state> clients_;
  error last_parse_error_;
};

} // namespace vast

#endif
