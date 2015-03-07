#include "vast/actor/search.h"

#include <caf/all.hpp>
#include "vast/expression.h"
#include "vast/query_options.h"
#include "vast/actor/query.h"
#include "vast/expr/normalize.h"

namespace vast {

using namespace caf;

search::search()
  : default_actor{"search"}
{
}

void search::on_exit()
{
  archive_ = invalid_actor;
  index_ = invalid_actor;
  queries_.clear();
}

behavior search::make_behavior()
{
  trap_exit(true);
  return
  {
    [=](exit_msg const& msg)
    {
      for (auto& q : queries_)
        link_to(q.second);
      quit(msg.reason);
    },
    [=](down_msg const& msg)
    {
      VAST_INFO(this, "got disconnect from client", msg.source);
      auto er = queries_.equal_range(msg.source);
      auto i = er.first;
      while (i != er.second)
      {
        VAST_DEBUG(this, "sends EXIT to query", i->second);
        send_exit(i->second, msg.reason);
        i = queries_.erase(i);
      }
    },
    [=](add_atom, archive_atom, actor const& a)
    {
      VAST_DEBUG(this, "adds archive", a);
      if (archive_ == invalid_actor)
      {
        archive_ = actor_pool::make(actor_pool::broadcast{});
        link_to(archive_);
      }
      send(archive_, sys_atom::value, put_atom::value, a);
      return ok_atom::value;
    },
    [=](add_atom, index_atom, actor const& a)
    {
      VAST_DEBUG(this, "adds index", a);
      if (index_ == invalid_actor)
      {
        index_ = actor_pool::make(actor_pool::broadcast{});
        link_to(index_);
      }
      send(index_, sys_atom::value, put_atom::value, a);
      return ok_atom::value;
    },
    [=](std::string const& str, query_options opts, actor const& client)
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
      if (! (has_historical_option(opts) || has_continuous_option(opts)))
      {
        return make_message(error{"no query mode specified"});
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
      queries_.emplace(client->address(), qry);
      send(index_, *expr, opts, qry);
      return make_message(*expr, qry);
    },
    catch_unexpected()
  };
}

} // namespace vast
