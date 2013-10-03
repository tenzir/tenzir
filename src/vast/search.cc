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
                auto q = spawn<query, linked>(
                    archive_, index_, client, std::move(ex));
                q->link_to(client);
                assert(queries_.count(q) == 0);
                send(creator, q);
                queries_.insert(q);
              }
              catch (error::query const& e)
              {
                std::stringstream msg;
                msg << "got invalid query expression: " << e.what();
                VAST_LOG_ACTOR_ERROR(msg.str());
                send(creator, actor_ptr{});
              }
            });
      });
}

char const* search::description() const
{
  return "search";
}

} // namespace vast
