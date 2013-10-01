#include "vast/search.h"

#include "vast/exception.h"
#include "vast/query.h"
#include "vast/util/make_unique.h"

namespace vast {

using namespace cppa;

search::search(actor_ptr archive, actor_ptr index, actor_ptr schema_manager)
  : archive_(archive)
  , index_(index)
  , schema_manager_(schema_manager)
{
}

void search::act()
{
  become(
      on(atom("query"), atom("create"), arg_match)
        >> [=](std::string const& str, actor_ptr client)
      {
        auto creator = last_sender();
        sync_send(schema_manager_, atom("schema")).then(
            on_arg_match >> [=](schema const& sch)
            {
              try
              {
                expression ex = expression::parse(str, sch);
                auto q = spawn<query>(archive_, index_, client, std::move(ex));
                assert(queries_.find(q) == queries_.end());
                queries_.emplace(q, client);
                monitor(client);
                send(creator, q);
              }
              catch (error::query const& e)
              {
                std::stringstream msg;
                msg << "got invalid query expression: " << e.what();
                VAST_LOG_ACTOR_ERROR(msg.str());
                send(creator, actor_ptr{});
              }
            });
      },
      on(atom("DOWN"), arg_match) >> [=](uint32_t /* reason */)
      {
        VAST_LOG_ACTOR_VERBOSE("noticed client @" << last_sender()->id() <<
                               " went down, removing associated queries");
        for (auto i = queries_.begin(); i != queries_.end(); )
        {
          if (i->second == last_sender())
          {
            VAST_LOG_ACTOR_DEBUG("erases query @" << i->first->id());
            send(i->first, atom("kill"));
            i = queries_.erase(i);
          }
          else
          {
            ++i;
          }
        }
      },
      on(atom("kill")) >> [=]
      {
        for (auto& i : queries_)
        {
          i.first << last_dequeued();
          VAST_LOG_ACTOR_VERBOSE("shuts down query @" << i.first->id());
        }
        queries_.clear();
        quit();
      });
}

char const* search::description() const
{
  return "search";
}

} // namespace vast
