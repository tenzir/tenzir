#include "vast/search.h"

#include <ze/util/make_unique.h>
#include "vast/logger.h"
#include "vast/query.h"

namespace vast {

search::search(cppa::actor_ptr archive, cppa::actor_ptr index)
  : archive_(archive)
  , index_(index)
{
  LOG(verbose, core) << "spawning search @" << id();
  using namespace cppa;
  init_state = (
      on(atom("query"), atom("create"), arg_match)
        >> [=](std::string const& expression)
      {
        auto client = last_sender();
        auto q = spawn<query>(archive_, index_, client);
        handle_response(sync_send(q, atom("parse"), expression))(
            on(atom("parse"), atom("failure")) >> [=]
            {
              send(q, atom("shutdown"));
            },
            on(atom("parse"), atom("success")) >> [=]
            {
              assert(queries_.find(q) == queries_.end());
              queries_.emplace(q, client);
              monitor(client);
              send(index_, atom("give"), q);
            },
            after(std::chrono::seconds(1)) >> [=]
            {
              LOG(error, query)
                << "search @" << id()
                << " did not receive parse answer from query @" << q->id();
              send(q, atom("shutdown"));
            });

      },
      on(atom("DOWN"), arg_match) >> [=](uint32_t reason)
      {
        LOG(verbose, query)
          << "client @" << last_sender()->id() << " went down,"
          << " removing all associated queries";

        for (auto i = queries_.begin(); i != queries_.end(); )
        {
          if (i->second == last_sender())
          {
            DBG(query)
              << "search @" << id() << " erases query @" << i->first->id();

            send(i->first, atom("shutdown"));
            i = queries_.erase(i);
          }
          else
          {
            ++i;
          }
        }
      },
      on(atom("shutdown")) >> [=]
      {
        for (auto& i : queries_)
        {
          LOG(debug, query)
            << "search @" << id() << " shuts down query @" << i.first->id();

          i.first << last_dequeued();
        }

        queries_.clear();
        quit();
        LOG(verbose, query) << "search @" << id() << " terminated";
      });
}

} // namespace vast
