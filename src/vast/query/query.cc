#include "vast/query/query.h"

#include <ze/event.h>
#include <ze/type/regex.h>
#include "vast/query/ast.h"
#include "vast/query/exception.h"
#include "vast/query/parser/query.h"
#include "vast/util/parser/parse.h"
#include "vast/util/logger.h"

namespace vast {
namespace query {

query::query(std::string str)
  : id_(ze::uuid::random())
  , state_(invalid)
  , str_(std::move(str))
{
    LOG(verbose, query) << "new query " << id_ << ": " << str_;

    ast::query query_ast;
    if (! util::parser::parse<parser::query>(str_, query_ast))
        throw syntax_exception(str_);

    state_ = parsed;

    if (! ast::validate(query_ast))
        throw semantic_exception("semantic error", str_);

    state_ = validated;

    expr_.assign(query_ast);
}

query::query(query&& other)
  : id_(std::move(other.id_))
  , state_(other.state_)
  , str_(std::move(other.str_))
  , expr_(std::move(other.expr_))
{
    other.state_ = invalid;
}

query& query::operator=(query other)
{
    using std::swap;
    swap(id_, other.id_);
    swap(state_, other.state_);
    swap(str_, other.str_);
    swap(expr_, other.expr_);
    return *this;
}

bool query::match(ze::event_ptr event)
{
    expr_.reset();
    return event->any(
        [&](ze::value const& value)
        {
            expr_.feed(value);
            return bool(expr_);
        });
}

query::state query::status() const
{
    return state_;
}

void query::status(query::state s)
{
    state_ = s;
}

ze::uuid const& query::id() const
{
    return id_;
}

} // namespace query
} // namespace vast
