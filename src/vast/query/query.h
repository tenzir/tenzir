#ifndef VAST_QUERY_QUERY_H
#define VAST_QUERY_QUERY_H

#include <string>
#include <cppa/cppa.hpp>
#include "vast/query/expression.h"

namespace vast {
namespace query {

/// The query.
class query : public ze::object, cppa::sb_actor<query>
{
public:
  struct statistics
  {
    uint64_t processed = 0ull;
    uint64_t matched = 0ull;
  };

  /// Constructs a query from a query expression.
  /// @param str The query expression.
  query(std::string str);

private:
  cppa::actor_ptr search_;
  cppa::actor_ptr source_;
  cppa::actor_ptr sink_;

  std::string str_;
  expression expr_;
  uint64_t batch_size_;
  statistics stats_;
};

} // namespace query
} // namespace vast

#endif
