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
  
  cppa::actor_ptr archive_;
  cppa::actor_ptr index_;
  cppa::actor_ptr schema_manager_;
  // Keeps track of all partial ASTs.
  std::map<expr::ast, state> state_;
  // Maps all query ASTs to <query, client> actors.
  std::multimap<expr::ast, std::pair<cppa::actor_ptr, cppa::actor_ptr>> actors_;
};

} // namespace vast

#endif
