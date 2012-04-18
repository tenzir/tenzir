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

query::query(ze::component& c,
             std::string str,
             uint64_t batch_size,
             batch_function each_batch)
  : device(c)
  , state_(invalid)
  , str_(std::move(str))
  , batch_size_(batch_size)
  , each_batch_(each_batch)
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
}

void query::relay()
{
    frontend().receive(
        [&](ze::event_ptr&& e)
        {
            ++stats_.processed;
            if (match(*e))
            {
                backend().send(*e);
                if (++stats_.matched % batch_size_ == 0ull && each_batch_)
                    each_batch_();
            }
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

void query::status(state s)
{
    state_ = s;
}

query::statistics const& query::stats() const
{
    return stats_;
}

} // namespace query
} // namespace vast
