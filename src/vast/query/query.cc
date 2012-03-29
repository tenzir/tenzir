#include "vast/query/query.h"

#include <ze/event.h>
#include <boost/uuid/random_generator.hpp>

namespace vast {
namespace query {

query::query(std::string const& expr)
  : id_(boost::uuids::random_generator()())
  , state_(unknown)
{

}

query::state query::status() const
{
    return state_;
}

void query::status(query::state s)
{
    state_ = s;
}

util::uuid query::id() const
{
    return id_;
}

bool match(query const& q, ze::event_ptr e)
{
    assert(! "not yet implemented");
    return true;
}

} // namespace query
} // namespace vast
