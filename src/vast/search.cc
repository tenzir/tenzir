#include "vast/search.h"

#include "vast/exception.h"
#include "vast/query.h"
#include "vast/util/make_unique.h"

using namespace cppa;

namespace vast {

search::search(actor_ptr archive, actor_ptr index, actor_ptr schema_manager)
  : archive_{std::move(archive)},
    index_{std::move(index)},
    schema_manager_{std::move(schema_manager)}
{
}

void search::act()
{
  become(
      on(atom("DOWN"), arg_match) >> [=](uint32_t reason)
      {
        auto& client = last_sender();
        VAST_LOG_ACTOR_INFO("got DOWN from client @" << client->id());
        assert(queries_.count(client));
        // TODO: don't shut down the queries, but rather put them in a detached
        // state so that their results can be reused by related queries.
        send_exit(queries_[client], reason);
        queries_.erase(client);
      },
      on(atom("query"), atom("create"), arg_match) >> [=](std::string const& q)
      {
        auto client = last_sender();
        try
        {
          auto qry =
            spawn<query>(archive_, index_, client, expression::parse(q));
          monitor(client);
          assert(queries_.count(client) == 0);
          queries_.emplace(client, qry);
          reply(qry);
        }
        catch (error::query const& e)
        {
          std::stringstream msg;
          msg << "got invalid query expression: " << e.what();
          VAST_LOG_ACTOR_ERROR(msg.str());
          reply(actor_ptr{});
        }
      });
}

char const* search::description() const
{
  return "search";
}

} // namespace vast
