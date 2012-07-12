#include "vast/query/search.h"

#include <ze/util/make_unique.h>
#include "vast/util/logger.h"
#include "vast/query/exception.h"
#include "vast/store/archive.h"
#include "vast/store/emitter.h"

namespace vast {
namespace query {

search::search(cppa::actor_ptr archive)
  : archive_(archive)
{
  using namespace cppa;
  init_state = (
      on(atom("query"), atom("create"), arg_match)
        >> [=](std::string const& expression)
      {
        auto q = spawn<query>(expression);
        send(archive_, atom("emitter"), atom("create"), q);
        auto sink = self->last_sender();
        become(
            keep_behavior,
            on(atom("emitter"), atom("create"), atom("ack"), arg_match)
              >> [=](actor_ptr emitter)
            {
              send(q, atom("set"), atom("source"), emitter);
              send(q, atom("set"), atom("sink"), sink);

              auto i = std::find(queries_.begin(), queries_.end(), q);
              assert(i == queries_.end());
              queries_.push_back(std::move(q));

              send(sink, atom("query"), atom("create"), atom("ack"), q);
              unbecome();
            },
            others() >> [=]
            {
              shutdown_query(q);
              unbecome();
            });
      },
      on(atom("query"), atom("remove"), arg_match) >> [=](actor_ptr q)
      {
        shutdown_query(q);
      },
      on(atom("shutdown")) >> [=]
      {
        for (auto& q : queries_)
          send(q, atom("shutdown"));

        queries_.clear();
      });
}

void search::shutdown_query(cppa::actor_ptr q)
{
  using namespace cppa;
  send(q, atom("shutdown"));
  queries_.erase(std::remove(queries_.begin(), queries_.end(), q),
                 queries_.end());
}

} // namespace query
} // namespace vast
