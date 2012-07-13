#include <vast/query/query.h>

#include <ze/chunk.h>
#include <ze/event.h>
#include <ze/type/regex.h>
#include <vast/query/ast.h>
#include <vast/query/exception.h>
#include <vast/query/parser/query.h>
#include <vast/util/parser/parse.h>
#include <vast/util/logger.h>

namespace vast {
namespace query {

query::query(std::string str)
  : str_(std::move(str))
{
  using namespace cppa;
  LOG(verbose, query) << "new query " << id() << ": " << str_;

  init_state = (
      on(atom("parse")) >> [=]
      {
        try
        {
          ast::query query_ast;
          if (! util::parser::parse<parser::query>(str_, query_ast))
            throw syntax_exception(str_);

          if (! ast::validate(query_ast))
            throw semantic_exception("semantic error", str_);

          expr_.assign(query_ast);
        }
        catch (syntax_exception const& e)
        {
          LOG(error, query)
            << "syntax error in query " << id() << ": " << e.what();

          reply(atom("query"), atom("parse"), atom("failure"), id());
        }
        catch (semantic_exception const& e)
        {
          LOG(error, query)
            << "semantic error in query " << id() << ": " << e.what();

          reply(atom("query"), atom("parse"), atom("failure"), id());
        }
      },
      on(atom("set"), atom("source"), arg_match) >> [=](actor_ptr source)
      {
        LOG(debug, query) << "setting source for query " << id();
        source_ = source;
      },
      on(atom("set"), atom("sink"), arg_match) >> [=](actor_ptr sink)
      {
        LOG(debug, query) << "setting sink for query " << id();
        sink_ = sink;
      },
      on(atom("set"), atom("batch size"), arg_match) >> [=](unsigned batch_size)
      {
        assert(batch_size > 0);

        LOG(debug, query)
          << "setting batch size to " << batch_size << " for query " << id();

        batch_size_ = batch_size;
      },
      on(atom("get"), atom("statistics")) >> [=]
      {
        reply(atom("statistics"), stats_.processed, stats_.matched);
      },
      on(atom("next batch")) >> [=]
      {
        send(source_, atom("emit"));
      },
      on_arg_match >> [=](ze::chunk<ze::event> const& chunk)
      {
        auto more = true;
        chunk.get().get([&more, this](ze::event e)
        {
          ++stats_.processed;
          if (expr_.eval(e))
          {
            sink_ << self->last_dequeued();
            if (++stats_.matched % batch_size_ == 0)
            {
              send(source_, atom("pause"));
              more = false;
            }
          }
        });

        if (more)
          send(self, atom("next batch"));
      },
      on(atom("shutdown")) >> [=]
      {
        LOG(debug, query) << "shutting down query " << id();
        self->quit();
      });
}

} // namespace query
} // namespace vast
