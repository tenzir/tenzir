#ifndef VAST_SEARCH_H
#define VAST_SEARCH_H

#include <unordered_map>
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
  // Maps queries to clients.
  std::unordered_map<cppa::actor_ptr, cppa::actor_ptr> queries_;

  cppa::actor_ptr archive_;
  cppa::actor_ptr index_;
  cppa::actor_ptr schema_manager_;
};

} // namespace vast

#endif
