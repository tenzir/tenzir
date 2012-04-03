#include "vast/query/query.h"

#include <ze/event.h>
#include <ze/type/regex.h>
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
  , state_(invalid)
{
    LOG(verbose, query) << "new query " << id_ << ": " << str;

    if (! util::parser::parse<parser::query>(str, ast_))
        throw syntax_exception(str);

    state_ = parsed;

    // TODO: canonify (e.g., fold constants).

    if (! ast::validate(ast_))
        throw semantic_exception("semantic error", str);

    state_ = validated;
}

query::query(query&& other)
  : id_(std::move(other.id_))
  , state_(other.state_)
  , ast_(std::move(other.ast_))
{
    other.state_ = invalid;
}

query& query::operator=(query other)
{
    using std::swap;
    swap(id_, other.id_);
    swap(state_, other.state_);
    swap(ast_, other.ast_);
    return *this;
}

bool query::match(ze::event_ptr event)
{
    boolean_expression expr(ast_);
    for (auto& arg : event->args())
        expr.feed(arg);

    return bool(expr);
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

} // namespace query
} // namespace vast
