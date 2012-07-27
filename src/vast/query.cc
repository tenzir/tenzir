#include "vast/query.h"

#include <ze/chunk.h>
#include <ze/event.h>
#include <ze/type/regex.h>
#include "vast/exception.h"
#include "vast/logger.h"
#include "vast/detail/ast.h"
#include "vast/detail/parser/query.h"
#include "vast/util/parser/parse.h"

namespace vast {

query::query(cppa::actor_ptr archive,
             cppa::actor_ptr index,
             cppa::actor_ptr sink,
             std::string str)
  : str_(std::move(str))
  , archive_(archive)
  , index_(index)
  , sink_(sink)
{
  using namespace cppa;
  LOG(verbose, query) 
    << "spawning query @" << id() 
    << " with expression \"" << str_ << '"'
    << " for sink @" << sink_->id();

  try
  {
    detail::ast::query query_ast;
    if (! util::parser::parse<detail::parser::query>(str_, query_ast))
      throw error::syntax("parse error", str_);

    if (! detail::ast::validate(query_ast))
      throw error::semantic("parse error", str_);

    expr_.assign(query_ast);

  }
  catch (error::syntax const& e)
  {
    LOG(error, query)
      << "syntax error in query @" << id() << ": " << e.what();

    reply(atom("query"), atom("parse"), atom("failure"), id());
  }
  catch (error::semantic const& e)
  {
    LOG(error, query)
      << "semantic error in query @" << id() << ": " << e.what();

    reply(atom("query"), atom("parse"), atom("failure"), id());
  }

  init_state = (
      on(atom("start")) >> [=]
      {
        send(index_, atom("give"), self);
      },
      on(atom("source"), arg_match) >> [=](actor_ptr source)
      {
        LOG(debug, query) 
          << "query @" << id() << " sets source to @" << source->id();

        source_ = source;
        send(sink_, atom("query"), atom("created"), self);
      },
      on(atom("set"), atom("batch size"), arg_match) >> [=](unsigned batch_size)
      {
        LOG(debug, query)
          << "query @" << id() << " sets batch size to " << batch_size;

        assert(batch_size > 0);
        batch_size_ = batch_size;
        reply(atom("set"), atom("batch size"), atom("ack"));
      },
      on(atom("get"), atom("statistics")) >> [=]
      {
        reply(atom("statistics"), stats_.processed, stats_.matched);
      },
      on(atom("next chunk")) >> [=]
      {
        LOG(debug, query)
          << "query @" << id() << " asks source @" << source_->id() 
          << " for next chunk";
        send(source_, atom("emit"));
      },
      on_arg_match >> [=](ze::chunk<ze::event> const& chunk)
      {
        auto need_more = true;
        chunk.get().get([&need_more, this](ze::event e)
        {
          ++stats_.processed;
          if (expr_.eval(e))
          {
            send(sink_, std::move(e));
            if (++stats_.matched % batch_size_ == 0)
              need_more = false;
          }
        });

        if (need_more)
          send(self, atom("next chunk"));
      },
      on(atom("finished")) >> [=]
      {
        send(sink_, atom("query"), atom("finished"));
      },
      on(atom("shutdown")) >> [=]
      {
        self->quit();
        LOG(verbose, query) << "query @" << id() << " terminated";
      });
}

} // namespace vast
