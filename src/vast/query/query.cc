#include "vast/query/query.h"

#include <ze/event.h>
#include <boost/uuid/random_generator.hpp>
#include "vast/query/parser/query.h"
#include "vast/util/parser/parse.h"

namespace vast {
namespace query {

query::query(std::string const& str)
  : id_(boost::uuids::random_generator()())
  , state_(unknown)
{
    ast::query query_ast;
    auto success = util::parser::parse<parser::query>(str, query_ast);

    if (! success)
        throw "query parse error"; // TODO throw correct exception
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
