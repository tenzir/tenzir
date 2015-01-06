#include "vast/search.h"

#include <caf/all.hpp>
#include "vast/expression.h"
#include "vast/query.h"
#include "vast/replicator.h"
#include "vast/expr/normalizer.h"

namespace vast {

using namespace caf;

search::search()
{
  attach_functor(
      [=](uint32_t reason)
      {
        archive_ = invalid_actor;
        index_ = invalid_actor;
        for (auto& p : clients_)
          for (auto& q : p.second.queries)
            anon_send_exit(q, reason);
      });
}

message_handler search::make_handler()
{
  return
  {
    [=](down_msg const& d)
    {
      VAST_INFO(this, "got disconnect from client", last_sender());

      for (auto& q : clients_[last_sender()].queries)
      {
        VAST_DEBUG(this, "sends EXIT to query", q);
        send_exit(q, d.reason);
      }

      clients_.erase(last_sender());
    },
    on(atom("add"), atom("archive"), arg_match) >> [=](actor const& a)
    {
      if (! archive_)
        archive_ = spawn<replicator, linked>();

      send(archive_, atom("add"), atom("worker"), a);
      return make_message(atom("ok"));
    },
    on(atom("add"), atom("index"), arg_match) >> [=](actor const& a)
    {
      if (! index_)
        index_ = spawn<replicator, linked>();

      send(index_, atom("add"), atom("worker"), a);
      return make_message(atom("ok"));
    },
    on(atom("query"), arg_match)
      >> [=](actor const& client, std::string const& str)
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

      auto ast = to<expression>(str);
      if (! ast)
      {
         VAST_VERBOSE(this, "ignores invalid query:", str);
         return make_message(ast.error());
      }

      *ast = visit(expr::normalizer{}, *ast);

      monitor(client);
      auto qry = spawn<query>(archive_, client, *ast);
      clients_[client.address()].queries.insert(qry);
      send(index_, atom("query"), *ast, qry);

      return make_message(*ast, qry);
    }
  };
}

std::string search::name() const
{
  return "search";
}

} // namespace vast
