#include "vast/query/search.h"

#include <ze/util/make_unique.h>
#include "vast/util/logger.h"
#include "vast/query/exception.h"
#include "vast/store/archive.h"
#include "vast/store/emitter.h"

namespace vast {
namespace query {

search::search(cppa::actor_ptr archive)
  : archive_(archive)
{
  using namespace cppa;
  init_state = (
      on(atom("publish"), arg_match) >> [](std::string const& host,
                                           uint16_t port)
      {
        LOG(info, query) 
          << "search component listening on " << host << ':' << port;

        publish(self, port);
      },
      on(atom("query"), atom("create"), arg_match) >> [](std::string const&
                                                         expression)
      {
        auto q = spawn<query>(self, expr);

        send(archive_, atom("emitter"), atom("create"), q);

        assert(queries_.find(q->id()) == queries.end());
        queries_.insert({q->id(), q})

      },
      on<atom("query"), atom("set"), arg_match> >> [](std::string const& id,
                                                      std::string const& opt,
                                                      std::vector<std::string> const& vals)
      {
        auto q = queries_.find(id);
        if (q == queries_.end())
        {
          reply("query", atom("error"), id, "invalid query id");
          return;
        }

        if (opt == "sink")
        {
          

        }
          send(*

      },

    on<atom("query"), atom("set"), std::string, anything>() >> 
      [](std::string const& id, )
      {
      }



                LOG(info, query) << "connecting to client " << dst;
                q->backend().connect(ze::zmq::tcp, dst);

                auto id = q->id();
                {
                    std::lock_guard<std::mutex> lock(query_mutex_);
                    queries_.emplace(id, std::move(q));
                    query_to_emitter_.emplace(id, emitter->id());
                }

                LOG(debug, query) << "sending query details back to client";
                ack(route, "query created", id.to_string());
            }
            else if (action == "remove" ||
                     action == "control" ||
                     action == "statistics")
            {
                auto i = options.find("id");
                if (i == options.end())
                {
                    nack(route, "'id' option required");
                    return;
                }

                auto id_string = i->second.get<ze::string>().to_string();
                if (id_string.empty())
                {
                    nack(route, "invalid query ID", id_string);
                    return;
                }

                auto qid = ze::uuid(id_string);
                std::lock_guard<std::mutex> lock(query_mutex_);
                auto q = queries_.find(qid);
                if (q == queries_.end())
                {
                    nack(route, "unknown query ID", qid.to_string());
                    return;
                }

                auto e = query_to_emitter_.find(qid);
                assert(e != query_to_emitter_.end());
                auto eid = e->second;

                if (action == "remove")
                {
                    LOG(info, query) << "removing query " << qid;
                    LOG(info, query) << "removing emitter " << eid;
                    archive_.remove_emitter(eid);
                    queries_.erase(q);
                    query_to_emitter_.erase(e);
                }
                else if (action == "control")
                {
                    auto emitter = archive_.lookup_emitter(eid);

                    auto a = options.find("aspect");
                    if (a == options.end())
                    {
                        nack(route, "'aspect' option required");
                        return;
                    }

                    auto aspect = a->second.get<ze::string>().to_string();
                    if (aspect == "next batch")
                    {
                        if (emitter->status() == store::emitter::finished)
                        {
                            nack(route, "query finished", qid.to_string());
                            return;
                        }

                        LOG(debug, query) << "starting emitter for next batch " << eid;
                        emitter->start();
                    }
                    else
                    {
                        nack(route, "unknown control aspect");
                        return;
                    }
                }
                else if (action == "statistics")
                {
                    auto& stats = q->second->stats();

                    auto selectivity = static_cast<double>(stats.matched) /
                                       static_cast<double>(stats.processed);

                    LOG(debug, query) << "sending query statistics to client";
                    ack(route,
                        "statistics",
                        qid.to_string(),
                        ze::table{"processed", std::to_string(stats.processed),
                                  "matches", std::to_string(stats.matched),
                                  "selectivity", std::to_string(selectivity)});
                    return;
                }
            }
        });
}

void search::init(std::string const& host, unsigned port)
{
}

void search::stop()
{
    query_to_emitter_.clear();
    queries_.clear();
};

} // namespace query
} // namespace vast
