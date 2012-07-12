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
  search(cppa::actor_ptr archive);
  
private:
  cppa::behavior init_state;

  cppa::actor_ptr archive_;
  std::unordered_map<ze::uuid, cppa::actor_ptr> queries_;
};

} // namespace query
} // namespace vast

#endif
