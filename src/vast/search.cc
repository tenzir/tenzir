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

  auto parse_ast = [=](std::string const& str) -> optional<expression>
  {
    auto ast = to<expression>(str);
    if (! ast)
    {
      last_parse_error_ = ast.error();
      return {};
    }

    *ast = visit(expr::normalizer{}, *ast);

    // Just test whether the AST resolves according to the schema. We don't use
    // the resolved AST here, though, as INDEX still needs the unresolved
    // version.
    auto t = visit(expr::schema_resolver{schema_}, *ast);
    if (! t)
    {
      last_parse_error_ = t.error();
      return {};
    }

    return std::move(*ast);
  };

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
        //VAST_LOG_ACTOR_DEBUG(schema_);

        auto t = io::archive(schema_path, schema_);
        if (t)
          VAST_LOG_ACTOR_VERBOSE("archived schema to " << schema_path);
        else
          VAST_LOG_ACTOR_ERROR("failed to write schema to " << schema_path);
      }
    },
    on(atom("query"), val<actor>, parse_ast)
      >> [=](actor const& client, expression const& ast) -> continue_helper
    {
      VAST_LOG_ACTOR_INFO("got client " << client << " asking for " << ast);

      // Must succeed because we checked it in parse_ast(). But because we want
      // to use both the resolved and original AST in this handler, we have to
      // do the resolving twice.
      auto resolved = visit(expr::schema_resolver{schema_}, ast);
      assert(resolved);

      auto qry = spawn<query>(archive_, client, std::move(*resolved));

      return sync_send(index_, atom("query"), ast, qry).then(
          on(atom("success")) >> [=]
          {
            monitor(client);
            clients_[client.address()].queries.insert(qry);
            return make_message(ast, qry);
          },
          [=](error const&)
          {
            send_exit(qry, exit::error);
            return last_dequeued();
          });
    },
    on(atom("query"), val<actor>, arg_match)
      >> [=](actor const&, std::string const& q)
    {
      VAST_LOG_ACTOR_VERBOSE("ignores invalid query: " << q);
      return make_message(last_parse_error_);
    }
  };
}

std::string search_actor::describe() const
{
  return "search";
}

} // namespace vast
