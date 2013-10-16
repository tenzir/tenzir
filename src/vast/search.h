#ifndef VAST_SEARCH_H
#define VAST_SEARCH_H

#include <cppa/cppa.hpp>
#include "vast/actor.h"
#include "vast/bitstream.h"
#include "vast/expression.h"

namespace vast {

struct search : actor<search>
{
  struct state
  {
    bitstream result;
    bitstream coverage;
    std::set<expr::ast> parents;
  };

  search(cppa::actor_ptr archive,
         cppa::actor_ptr index,
         cppa::actor_ptr schema_manager);

  void act();
  char const* description() const;
  
  void shutdown_client(cppa::actor_ptr const& client);

  cppa::actor_ptr archive_;
  cppa::actor_ptr index_;
  cppa::actor_ptr schema_manager_;
  std::multimap<cppa::actor_ptr, expr::ast> clients_;
  std::multimap<expr::ast, cppa::actor_ptr> queries_;
  std::map<expr::ast, state> state_;
};

} // namespace vast

#endif
