#include "vast/query/query.h"

#include <ze/event.h>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include "vast/query/exception.h"
#include "vast/query/parser/query.h"
#include "vast/util/parser/parse.h"
#include "vast/util/logger.h"

namespace vast {
namespace query {

query::query(std::string const& str)
  : id_(boost::uuids::random_generator()())
  , state_(unknown)
{
    LOG(verbose, query) << "new query " << id_ << ": " << str;

    ast::query query_ast;
    if (! util::parser::parse<parser::query>(str, query_ast))
        throw syntax_exception(str);
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
