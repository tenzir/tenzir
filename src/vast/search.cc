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
      on(atom("query"), atom("create")) >> [=]
      {
        auto q = spawn<query>(archive_, index_, last_sender());
        assert(queries_.find(q) == queries_.end());
        queries_.emplace(q, last_sender());
        monitor(last_sender());
        reply(atom("query"), q);
      },
      on(atom("DOWN"), arg_match) >> [=](uint32_t reason)
      {
        LOG(verbose, query)
          << "client @" << last_sender()->id() 
          << " went down, removing associated queries";

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
