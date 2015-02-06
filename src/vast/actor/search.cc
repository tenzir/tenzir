#include "vast/actor/search.h"

#include <caf/all.hpp>
#include "vast/expression.h"
#include "vast/actor/query.h"
#include "vast/actor/replicator.h"
#include "vast/expr/normalize.h"

namespace vast {

using namespace caf;

search::search()
{
  attach_functor(
      [=](uint32_t)
      {
        archive_ = invalid_actor;
        index_ = invalid_actor;
        clients_.clear();
      });
}

void search::at(exit_msg const& msg)
{
  for (auto& p : clients_)
    for (auto& q : p.second.queries)
    {
      VAST_DEBUG(this, "sends EXIT to query", q);
      send_exit(q, msg.reason);
    }
  quit(msg.reason);
}

void search::at(down_msg const& msg)
{
  VAST_INFO(this, "got disconnect from client", msg.source);
  for (auto& q : clients_[msg.source].queries)
  {
    VAST_DEBUG(this, "sends EXIT to query", q);
    send_exit(q, msg.reason);
  }
  clients_.erase(last_sender());
}

message_handler search::make_handler()
{
  return
  {
    [=](add_atom, archive_atom, actor const& a)
    {
      VAST_DEBUG(this, "adds archive", a);
      if (! archive_)
        archive_ = spawn<replicator, linked>();
      send(archive_, add_atom::value, worker_atom::value, a);
      return ok_atom::value;
    },
    [=](add_atom, index_atom, actor const& a)
    {
      VAST_DEBUG(this, "adds index", a);
      if (! index_)
        index_ = spawn<replicator, linked>();
      send(index_, add_atom::value, worker_atom::value, a);
      return ok_atom::value;
    },
    [=](query_atom, actor const& client, std::string const& str)
    {
      VAST_INFO(this, "got client", client, "asking for", str);
      if (! archive_)
      {
        quit(exit::error);
        return make_message(error{"no archive configured"});
      }
      else if (! index_)
      {
        quit(exit::error);
        return make_message(error{"no index configured"});
      }
      auto expr = to<expression>(str);
      if (! expr)
      {
         VAST_VERBOSE(this, "ignores invalid query:", str);
         return make_message(expr.error());
      }
      *expr = expr::normalize(*expr);
      VAST_DEBUG(this, "normalized query to", *expr);
      monitor(client);
      auto qry = spawn<query>(archive_, client, *expr);
      clients_[client.address()].queries.insert(qry);
      send(index_, *expr, qry);
      return make_message(*expr, qry);
    }
  };
}

std::string search::name() const
{
  return "search";
}

} // namespace vast
