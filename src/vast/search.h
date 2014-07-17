#ifndef VAST_SEARCH_H
#define VAST_SEARCH_H

#include <caf/all.hpp>
#include "vast/actor.h"
#include "vast/file_system.h"
#include "vast/schema.h"
#include "vast/util/flat_set.h"

namespace vast {

struct search_actor : actor_base
{
  struct client_state
  {
    util::flat_set<caf::actor> queries;
  };

  search_actor(path dir, caf::actor archive, caf::actor index);

  caf::message_handler act() final;
  std::string describe() const final;

  path dir_;
  schema schema_;
  caf::actor archive_;
  caf::actor index_;
  std::map<caf::actor_addr, client_state> clients_;
  error last_parse_error_;
};

} // namespace vast

#endif
