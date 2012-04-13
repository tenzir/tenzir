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

query::query(ze::component& c, std::string str)
  : device(c)
  , state_(invalid)
  , str_(std::move(str))
{
    LOG(verbose, query) << "new query " << id() << ": " << str_;

    ast::query query_ast;
    if (! util::parser::parse<parser::query>(str_, query_ast))
        throw syntax_exception(str_);

    state_ = parsed;

    if (! ast::validate(query_ast))
        throw semantic_exception("semantic error", str_);

    state_ = validated;

    expr_.assign(query_ast);

    //filter([&](ze::event_ptr const& e) { return match(*e); });
    frontend().receive(
        [&](ze::event_ptr&& e)
        {
            if (match(*e))
                backend().send(*e);
        });
}

bool query::match(ze::event const& event)
{
    expr_.reset();
    return event.any(
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

} // namespace query
} // namespace vast
