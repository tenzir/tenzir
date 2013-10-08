#ifndef VAST_SEARCH_H
#define VAST_SEARCH_H

#include <cppa/cppa.hpp>
#include "vast/actor.h"
#include "vast/bitstream.h"
#include "vast/expression.h"

namespace vast {

class search : public actor<search>
{
public:
  struct state
  {
    bitstream result;
    bitstream coverage;
    std::set<cppa::actor_ptr> queries;
    std::set<expr::ast> parents;
  };

  search(cppa::actor_ptr archive,
         cppa::actor_ptr index,
         cppa::actor_ptr schema_manager);

  void act();
  char const* description() const;
  
private:
  cppa::actor_ptr archive_;
  cppa::actor_ptr index_;
  cppa::actor_ptr schema_manager_;
  std::multimap<cppa::actor_ptr, expr::ast> clients_;
  std::map<expr::ast, cppa::actor_ptr> queries_;
  std::map<expr::ast, state> state_;
};

} // namespace vast

#endif
