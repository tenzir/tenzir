#include "vast/search.h"

#include "vast/expression.h"
#include "vast/optional.h"
#include "vast/query.h"
#include "vast/util/make_unique.h"
#include "vast/util/trial.h"

namespace vast {

using namespace cppa;

search_actor::search_actor(actor_ptr archive,
                           actor_ptr index,
                           actor_ptr schema_manager)
  : archive_{std::move(archive)},
    index_{std::move(index)},
    schema_manager_{std::move(schema_manager)}
{
}

void search_actor::act()
{
  trap_exit(true);

  auto parse_ast = [=](std::string const& str) -> optional<expr::ast>
  {
    auto ast = expr::ast::parse(str);
    if (ast)
      return std::move(*ast);

    last_parse_error_ = ast.failure().msg();
    return {};
  };

  become(
      on(atom("EXIT"), arg_match) >> [=](uint32_t reason)
      {
        for (auto& p : clients_)
        {
          for (auto& q : p.second.queries)
          {
            VAST_LOG_ACTOR_DEBUG("sends EXIT to query " << VAST_ACTOR_ID(q));
            send_exit(q, reason);
          }

          send(p.first, atom("exited"));
        }

        quit(reason);
      },
      on(atom("DOWN"), arg_match) >> [=](uint32_t reason)
      {
        VAST_LOG_ACTOR_DEBUG(
            "received DOWN from client " << VAST_ACTOR_ID(last_sender()));

        for (auto& q : clients_[last_sender()].queries)
        {
          VAST_LOG_ACTOR_DEBUG("sends EXIT to query " << VAST_ACTOR_ID(q));
          send_exit(q, reason);
        }

        clients_.erase(last_sender());
      },
      on(atom("query"), atom("create"), parse_ast)
        >> [=](expr::ast const& ast) -> continue_helper
      {
        auto client = last_sender();
        monitor(client);

        VAST_LOG_ACTOR_INFO("got new client " << VAST_ACTOR_ID(client) <<
                            " asking for " << ast);

        auto qry = spawn<query_actor>(archive_, client, ast);

        return sync_send(index_, atom("query"), ast, qry).then(
            on(atom("success")) >> [=]
            {
              clients_[client].queries.insert(qry);
              return make_any_tuple(ast, qry);
            },
            on(atom("error"), arg_match) >> [=](std::string const&)
            {
              send_exit(qry, exit::error);
              return last_dequeued();
            });
      },
      on(atom("query"), atom("create"), arg_match) >> [=](std::string const& q)
      {
        VAST_LOG_ACTOR_VERBOSE("ignores invalid query: " << q);

        return make_any_tuple(atom("error"), last_parse_error_);
      },
      others() >> [=]
      {
        VAST_LOG_ACTOR_ERROR("got unexpected message from " <<
                             VAST_ACTOR_ID(last_sender()) << ": " <<
                             to_string(last_dequeued()));
      });
}

char const* search_actor::description() const
{
  return "search";
}

} // namespace vast
