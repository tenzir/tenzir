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

query::query(cppa::actor_ptr search, std::string str)
  : search_(search)
  , state_(invalid)
  , str_(std::move(str))
{
  LOG(verbose, query) << "new query " << id() << ": " << str_;

  init_state = (
      on(atom("initialize"), arg_match) >> [](unsigned batch_size)
      {
        ast::query query_ast;
        if (! util::parser::parse<parser::query>(str_, query_ast))
          throw syntax_exception(str_);

        if (! ast::validate(query_ast))
          throw semantic_exception("semantic error", str_);

        expr_.assign(query_ast);
      },
      on(atom("set"), atom("source"), arg_match) >> [](actor_ptr source)
      {
        LOG(debug, query) << "setting source for query " << id();
        source_ = source;
      },
      on(atom("set"), atom("sink"), arg_match) >> [](actor_ptr sink)
      {
        LOG(debug, query) << "setting sink for query " << id();
        sink_ = sink;
      },
      on(atom("set"), atom("batch size"), arg_match) >> [](unsigned batch_size)
      {
        assert(batch_size > 0);

        LOG(debug, query) 
          << "setting batch size to " << batch_size << " for query " << id();

        batch_size_ = batch_size;
      },
      on(atom("get"), atom("statistics")) >> []()
      {
        // TODO: make query statistics a first-class libcppa citizen.
        //reply(stats_);
      },
      on_arg_match >> [](ze::event const& e)
      {
        ++stats_.processed;
        if (expr_.eval(e))
        {
          sink_ << self->last_dequeued();
          if (++stats_.matched % batch_size_ == 0ull)
            send(source_, atom("pause"));
        }
      });
}

} // namespace query
} // namespace vast
