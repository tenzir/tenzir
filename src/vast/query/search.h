#ifndef VAST_QUERY_SEARCH_H
#define VAST_QUERY_SEARCH_H

#include <unordered_map>
#include <cppa/cppa.hpp>
#include <ze/event.h>
#include "vast/query/query.h"

namespace vast {
namespace query {

class search : public cppa::sb_actor<search>
{
  friend class cppa::sb_actor<search>;

public:
  search(cppa::actor_ptr archive, cppa::actor_ptr index);
  
private:
  std::vector<cppa::actor_ptr> queries_;
  std::unordered_multimap<cppa::actor_ptr, cppa::actor_ptr> clients_;
  cppa::actor_ptr archive_;
  cppa::actor_ptr index_;
  cppa::behavior init_state;
};

} // namespace query
} // namespace vast

#endif
