#ifndef VAST_SEARCH_H
#define VAST_SEARCH_H

#include <set>
#include <cppa/cppa.hpp>
#include "vast/actor.h"

namespace vast {

class search : public actor<search>
{
public:
  search(cppa::actor_ptr archive,
         cppa::actor_ptr index,
         cppa::actor_ptr schema_manager);

  void act();
  char const* description() const;
  
private:
  cppa::actor_ptr archive_;
  cppa::actor_ptr index_;
  cppa::actor_ptr schema_manager_;
  std::set<cppa::actor_ptr> queries_;
};

} // namespace vast

#endif
