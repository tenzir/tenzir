#include "vast/search.h"

#include <ze/util/make_unique.h>
#include "vast/logger.h"
#include "vast/query.h"

namespace vast {

using namespace cppa;

search::search(actor_ptr archive, actor_ptr index, actor_ptr schema_manager)
  : archive_(archive)
  , index_(index)
  , schema_manager_(schema_manager)
{
  LOG(verbose, core) << "spawning search @" << id();
  init_state = (
      on(atom("query"), atom("create"), arg_match)
        >> [=](std::string const& str)
      {
        auto client = last_sender();
        sync_send(schema_manager, atom("schema")).then(
            on_arg_match >> [=](schema const& sch)
            {
              try
              {
                expression expr;
                expr.parse(str, sch);

                auto q = spawn<query>(
                    archive_, index_, client, std::move(expr));

                assert(queries_.find(q) == queries_.end());
                queries_.emplace(q, client);

                monitor(client);
                send(client, atom("query"), q);
              }
              catch (error::query const& e)
              {
                std::stringstream msg;
                msg << "query @" << id() << " is invalid: " << e.what();
                LOG(error, query) << msg.str();
                send(client, atom("query"), atom("failure"), msg.str());
              }

            },
            after(std::chrono::seconds(1)) >> [=]
            {
              std::stringstream msg;
              msg << "query @" << id()
                << " timed out trying to reach schema manager @"
                << schema_manager->id();
              LOG(error, query) << msg.str();
              send(client, atom("query"), atom("failure"), msg.str());
            });
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
