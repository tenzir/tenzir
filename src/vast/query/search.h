#ifndef VAST_QUERY_SEARCH_H
#define VAST_QUERY_SEARCH_H

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
  void shutdown_query(cppa::actor_ptr q);

  std::vector<cppa::actor_ptr> queries_;
  cppa::actor_ptr archive_;
  cppa::behavior init_state;
};

} // namespace query
} // namespace vast

#endif
