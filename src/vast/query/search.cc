#include "vast/query/search.h"

#include <ze/util/make_unique.h>
#include "vast/util/logger.h"
#include "vast/query/exception.h"
#include "vast/store/archive.h"
#include "vast/store/emitter.h"

namespace vast {
namespace query {

search::search(ze::io& io, store::archive& archive)
  : ze::component(io)
  , archive_(archive)
  , manager_(*this)
{
    manager_.receive_with_route(
        [&](ze::event event, std::vector<ze::zmq::message> route)
        {
            if (event.name() != "VAST::query")
            {
                nack(route, "invalid query event name");
                return;
            }

            if (event.size() != 2)
            {
                nack(route, "invalid number of event arguments");
                return;
            }

            if (event[0].which() != ze::string_type ||
                event[1].which() != ze::table_type)
            {
                nack(route, "invalid event argument types");
                return;
            }

            auto action = event[0].get<ze::string>().to_string();
            auto& options = event[1].get<ze::table>();

            if (options.key_value_type() != ze::string_type ||
                options.map_value_type() != ze::string_type)
            {
                nack(route, "invalid 'options' key/value type");
                return;
            }

            if (action == "create")
            {
                auto e = options.find("expression");
                if (e == options.end())
                {
                    nack(route, "'expression' option required");
                    return;
                }

                auto d = options.find("destination");
                if (d == options.end())
                {
                    nack(route, "'destination' option required");
                    return;
                }

                auto expr = e->second.get<ze::string>().to_string();
                auto dst = d->second.get<ze::string>().to_string();

                auto& emitter = archive_.create_emitter();
                std::unique_ptr<query> q;

                auto b = options.find("batch size");
                if (b == options.end())
                    q = std::make_unique<query>(*this, expr);
                else
                    q = std::make_unique<query>(
                        *this,
                        expr,
                        std::strtoull(
                            b->second.get<ze::string>().data(), nullptr, 10),
                        [&] { emitter.pause(); });

                emitter.to(q->frontend());
                LOG(info, query) << "connecting to client " << dst;
                q->backend().connect(ze::zmq::tcp, dst);

                // FIXME: Find out why it makes a difference *when* we setup up
                // the subscriber-to-dealer relay. The emitter won't send any
                // messages to the query if we call this function before
                // connecting the emitter with the query frontend, i.e., before
                // emitter.to(q->frontend()). Ideally, this function should not
                // even exist and its code would move to the query constructor.
                q->relay();
                q->status(query::running);

                auto id = q->id();
                {
                    std::lock_guard<std::mutex> lock(query_mutex_);
                    queries_.emplace(id, std::move(q));
                    query_to_emitter_.emplace(id, emitter.id());
                }

                LOG(debug, query) << "sending query details back to client";
                ack(route, "query created", id.to_string());

                emitter.start();
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

                auto qid = ze::uuid(i->second.get<ze::string>().to_string());
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
                    archive.remove_emitter(eid);
                    queries_.erase(q);
                    query_to_emitter_.erase(e);
                }
                else if (action == "control")
                {
                    auto& emitter = archive_.lookup_emitter(eid);

                    auto a = options.find("aspect");
                    if (a == options.end())
                    {
                        nack(route, "'aspect' option required");
                        return;
                    }

                    auto aspect = a->second.get<ze::string>().to_string();
                    if (aspect == "next batch")
                    {
                        if (emitter.start() == store::emitter::finished)
                        {
                            nack(route, "query finished", qid.to_string());
                            return;
                        }
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
    auto endpoint = host + ":" + std::to_string(port);
    manager_.bind(ze::zmq::tcp, endpoint);
    LOG(info, query) << "search component listening on " << endpoint;
}

void search::stop()
{
    query_to_emitter_.clear();
    queries_.clear();
};

} // namespace query
} // namespace vast
