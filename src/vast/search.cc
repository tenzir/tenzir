#include "vast/search.h"

#include "vast/expression.h"
#include "vast/optional.h"
#include "vast/query.h"
#include "vast/expr/normalizer.h"
#include "vast/expr/resolver.h"
#include "vast/io/serialization.h"
#include "vast/util/trial.h"

namespace vast {

using namespace caf;

search_actor::search_actor(path dir, actor archive, actor index)
  : dir_{std::move(dir)},
    archive_{archive},
    index_{index}
{
}

message_handler search_actor::act()
{
  trap_exit(true);

  auto schema_path = dir_ / "schema";
  if (exists(schema_path))
  {
    auto t = io::unarchive(schema_path, schema_);
    if (t)
      VAST_LOG_ACTOR_VERBOSE("read schema from " << schema_path);
    else
      VAST_LOG_ACTOR_ERROR("failed to read schema from " << schema_path);
  }

  return
  {
    [=](exit_msg const& e)
    {
      for (auto& p : clients_)
      {
        for (auto& q : p.second.queries)
        {
          VAST_LOG_ACTOR_DEBUG("sends EXIT to query " << q);
          send_exit(q, e.reason);
        }
      }

      quit(e.reason);
    },
    [=](down_msg const& d)
    {
      VAST_LOG_ACTOR_INFO("got disconnect from client " << last_sender());

      for (auto& q : clients_[last_sender()].queries)
      {
        VAST_LOG_ACTOR_DEBUG("sends EXIT to query " << q);
        send_exit(q, d.reason);
      }

      clients_.erase(last_sender());
    },
    on(atom("schema")) >> [=]
    {
      return make_message(schema_);
    },
    [=](schema const& s)
    {
      auto sch = schema::merge(schema_, s);
      if (! sch)
      {
        VAST_LOG_ACTOR_ERROR(sch.error());
        send_exit(this, exit::error);
      }
      else if (*sch != schema_)
      {
        schema_ = *sch;
        VAST_LOG_ACTOR_DEBUG("successfully merged schemata");

        auto t = io::archive(schema_path, schema_);
        if (t)
          VAST_LOG_ACTOR_VERBOSE("archived schema to " << schema_path);
        else
          VAST_LOG_ACTOR_ERROR("failed to write schema to " << schema_path);
      }
    },
    on(atom("query"), arg_match)
      >> [=](actor const& client, std::string const& str)
    {
      VAST_LOG_ACTOR_INFO("got client " << client << " asking for " << str);

      auto ast = to<expression>(str);
      if (! ast)
      {
         VAST_LOG_ACTOR_VERBOSE("ignores invalid query: " << str);
         return make_message(ast.error());
      }

      *ast = visit(expr::normalizer{}, *ast);

      auto resolved = visit(expr::schema_resolver{schema_}, *ast);
      if (! resolved)
      {
        VAST_LOG_ACTOR_VERBOSE("could not resolve expression: " << resolved.error());
        return make_message(resolved.error());
      }

      monitor(client);
      auto qry = spawn<query>(archive_, client, std::move(*resolved));
      clients_[client.address()].queries.insert(qry);
      send(index_, atom("query"), *ast, qry);

      return make_message(*ast, qry);
    }
  };
}

std::string search_actor::describe() const
{
  return "search";
}

} // namespace vast
