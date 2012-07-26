#include <vast/query/search.h>

#include <ze/util/make_unique.h>
#include <vast/util/logger.h>
#include <vast/query/exception.h>
#include <vast/store/archive.h>
#include <vast/store/emitter.h>

namespace vast {
namespace query {

search::search(cppa::actor_ptr archive, cppa::actor_ptr index)
  : archive_(archive)
  , index_(index)
{
  LOG(verbose, core) << "spawning search @" << id();
  using namespace cppa;
  init_state = (
      on(atom("query"), atom("create"), arg_match)
        >> [=](std::string const& expression)
      {
        auto client = last_sender();
        monitor(client);
        auto q = spawn<query>(archive_, index_, client, expression);
        send(q, atom("start"));
        queries_.push_back(q);
        clients_.emplace(client, q);
      },
      on(atom("DOWN"), arg_match) >> [=](uint32_t reason)
      {
        auto client = last_sender();
        LOG(verbose, query) << "client @" << client->id() << " went down";
        auto range = clients_.equal_range(client);
        auto i = range.first;
        while (i != range.second)
        {
          auto x = i++;
          auto q = x->second;

          LOG(debug, query) << "search @" << id() 
            << " removes query @" << q->id();

          send(q, atom("shutdown"));
          clients_.erase(x);
          queries_.erase(std::remove(queries_.begin(), queries_.end(), q),
                         queries_.end());
        }
      },
      on(atom("shutdown")) >> [=]
      {
        for (auto& q : queries_)
        {
          LOG(debug, query) << "@" << id() << " shuts down query @" << q->id();
          q << last_dequeued();
        }

        queries_.clear();
        quit();
        LOG(verbose, query) << "search @" << id() << " terminated";
      });
}

} // namespace query
} // namespace vast
